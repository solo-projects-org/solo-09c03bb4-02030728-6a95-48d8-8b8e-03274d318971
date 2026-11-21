#!/usr/bin/env python3
"""灌装线欠重累计复检 + 留样台账条码配对。

复检部分：
上传「抽检序号,整数克重」CSV 存入 SQLite：
- 同号可更正；新号只允许往后追加（必须大于当前最大序号）。
- 按序号顺序把 (499 - 实测克重) 计入累计，累计 < 0 时归零；
  累计 >= 20 克时记录一条复检事件（本轮累计转正起点序号 -> 触发序号、累计克数），随后清零。
- 更正触发增量复算：跳过首处改动前的称重（直接取 run_state 保存的状态）；
  复算途中若已越过全部更正、当前序号仍是旧数据，且 (累计值, 本轮起点) 与旧结果一致，
  则之后的旧结果直接沿用，不再重读重量；若更后面还有新增序号，则从旧末尾状态继续处理。

留样台账部分：
- 从已有抽检序号中选一段（区间称重最多 200 条），导入按留样顺序排列的
  「条码,整数克重」CSV 建一份台账；
- 配对规则：条码不重复，按台账顺序对应递增的不同抽检序号，台账克重与称重最多差 1 克，
  未留样的称重可跳过；已现场核实的条码固定到核实序号（不再受克重差约束）；
- 配不齐整份台账时页面给出提示；存在多种完整配对时标出有歧义的条码；
- 质检员可登记/撤销现场核实（条码↔序号）；新核实若与已有记录一起配不齐整张表，
  本次登记被退回；
- 整份台账只有一种配对时才可发布，且提交配对须与页面所见一致；发布后可在台账页
  按序号查条码；
- 称重更正照常生效；更正或撤销核实一旦让原配对不再是唯一解，同次操作即暂停查袋，
  旧配对保留供追查，恢复查袋须重新发布。

每次上传/变更为一个事务，进程内全局锁 + BEGIN IMMEDIATE 串行处理；提交即持久化，
失败或重启后仍可查到最近一次成功结果。
"""

import csv
import io
import os
import sqlite3
import threading

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

NOMINAL_GRAMS = 499   # 标称克重
TRIGGER_GRAMS = 20    # 累计触发阈值（克）

LEDGER_MAX_WEIGHINGS = 200  # 一段台账最多覆盖的称重条数
LEDGER_MAX_ROWS = 200       # 一份台账最多的条码行数
WEIGHT_TOLERANCE = 1        # 台账克重与称重的允许差（克）

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    applied_at    TEXT NOT NULL DEFAULT (datetime('now')),
    rows          INTEGER NOT NULL,
    additions     INTEGER NOT NULL,
    corrections   INTEGER NOT NULL,
    first_changed INTEGER,
    resumed_at    INTEGER
);
CREATE TABLE IF NOT EXISTS weighings (
    seq    INTEGER PRIMARY KEY,
    weight INTEGER NOT NULL CHECK (weight >= 0)
);
CREATE TABLE IF NOT EXISTS run_state (
    seq         INTEGER PRIMARY KEY,
    cumulative  INTEGER NOT NULL,
    round_start INTEGER
);
CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id  INTEGER NOT NULL REFERENCES batches(id),
    start_seq INTEGER NOT NULL,
    end_seq   INTEGER NOT NULL,
    grams     INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS ledgers (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    start_seq  INTEGER NOT NULL,
    end_seq    INTEGER NOT NULL,
    status     TEXT NOT NULL DEFAULT 'draft'
               CHECK (status IN ('draft', 'published', 'paused'))
);
CREATE TABLE IF NOT EXISTS ledger_rows (
    ledger_id INTEGER NOT NULL REFERENCES ledgers(id),
    idx       INTEGER NOT NULL,
    barcode   TEXT NOT NULL,
    weight    INTEGER NOT NULL CHECK (weight >= 0),
    PRIMARY KEY (ledger_id, idx),
    UNIQUE (ledger_id, barcode)
);
CREATE TABLE IF NOT EXISTS verifications (
    ledger_id  INTEGER NOT NULL REFERENCES ledgers(id),
    barcode    TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (ledger_id, barcode)
);
CREATE TABLE IF NOT EXISTS publications (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ledger_id    INTEGER NOT NULL REFERENCES ledgers(id),
    published_at TEXT NOT NULL DEFAULT (datetime('now')),
    active       INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS publication_pairs (
    publication_id INTEGER NOT NULL REFERENCES publications(id),
    idx            INTEGER NOT NULL,
    barcode        TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    PRIMARY KEY (publication_id, idx)
);
"""

# 同一进程内串行化上传与台账变更；跨连接再由 BEGIN IMMEDIATE 的写锁兜底。
_upload_lock = threading.Lock()


# ---------------------------------------------------------------- CSV 解析

def parse_csv(text):
    """解析「序号,克重」CSV，允许首行表头。返回 [(seq, weight), ...]，非法即抛 ValueError。"""
    rows = []
    seen = set()
    reader = csv.reader(io.StringIO(text))
    for lineno, raw in enumerate(reader, start=1):
        if not raw or all(not cell.strip() for cell in raw):
            continue
        if len(raw) != 2:
            raise ValueError(f"第 {lineno} 行需要恰好两列（序号,克重），实际 {len(raw)} 列")
        a, b = raw[0].strip(), raw[1].strip()
        seq = weight = None
        try:
            seq = int(a)
        except ValueError:
            pass
        try:
            weight = int(b)
        except ValueError:
            pass
        if seq is None or weight is None:
            if lineno == 1 and seq is None and weight is None:
                continue  # 表头行
            raise ValueError(f"第 {lineno} 行不是整数：{a!r}, {b!r}")
        if seq < 1:
            raise ValueError(f"第 {lineno} 行序号必须为正整数：{seq}")
        if weight < 0:
            raise ValueError(f"第 {lineno} 行克重不能为负：{weight}")
        if seq in seen:
            raise ValueError(f"序号 {seq} 在文件内重复（第 {lineno} 行）")
        seen.add(seq)
        rows.append((seq, weight))
    if not rows:
        raise ValueError("CSV 中没有数据行")
    return rows


def parse_ledger_csv(text):
    """解析台账「条码,整数克重」CSV（按留样顺序），允许首行表头。

    返回 [(barcode, weight), ...]；条码在文件内不得重复，非法即抛 ValueError。
    """
    rows = []
    seen = set()
    reader = csv.reader(io.StringIO(text))
    for lineno, raw in enumerate(reader, start=1):
        if not raw or all(not cell.strip() for cell in raw):
            continue
        if len(raw) != 2:
            raise ValueError(f"第 {lineno} 行需要恰好两列（条码,克重），实际 {len(raw)} 列")
        barcode, w = raw[0].strip(), raw[1].strip()
        try:
            weight = int(w)
        except ValueError:
            if lineno == 1:
                continue  # 表头行
            raise ValueError(f"第 {lineno} 行克重不是整数：{w!r}")
        if not barcode:
            raise ValueError(f"第 {lineno} 行条码为空")
        if len(barcode) > 64:
            raise ValueError(f"第 {lineno} 行条码过长（超过 64 字符）")
        if any(ch.isspace() for ch in barcode) or any(
                ch in barcode for ch in ",:;"):
            raise ValueError(f"第 {lineno} 行条码含非法字符：{barcode!r}")
        if weight < 0:
            raise ValueError(f"第 {lineno} 行克重不能为负：{weight}")
        if barcode in seen:
            raise ValueError(f"条码 {barcode} 在文件内重复（第 {lineno} 行）")
        seen.add(barcode)
        rows.append((barcode, weight))
    if not rows:
        raise ValueError("CSV 中没有数据行")
    if len(rows) > LEDGER_MAX_ROWS:
        raise ValueError(f"一份台账最多 {LEDGER_MAX_ROWS} 行，实际 {len(rows)} 行")
    return rows


# ---------------------------------------------------------------- 复检核心计算

def _accumulate(states, events, cum, rs, seq, weight):
    """处理一条称重：累加差值、归零、触发复检事件。返回新的 (cum, rs)。"""
    cum += NOMINAL_GRAMS - weight
    if cum < 0:
        cum = 0
    if cum == 0:
        rs = None
    else:
        if rs is None:
            rs = seq          # 本轮累计转正的位置
        if cum >= TRIGGER_GRAMS:
            events.append((rs, seq, cum))
            cum, rs = 0, None
    states.append((seq, cum, rs))
    return cum, rs


def _insert_results(conn, batch_id, states, events):
    conn.executemany(
        "INSERT INTO run_state (seq, cumulative, round_start) VALUES (?,?,?)", states)
    conn.executemany(
        "INSERT INTO events (batch_id, start_seq, end_seq, grams) VALUES (?,?,?,?)",
        [(batch_id, s, e, g) for s, e, g in events])


def _recompute(conn, changed, batch_id, old_max):
    """从首个改动序号起增量复算。changed 为实际变化的序号（新增或克重改变），已排序。

    - 跳过首处改动前的称重：直接取 run_state 中保存的改动前状态。
    - 复算途中若已越过全部更正、当前序号仍属旧数据，且 (累计值, 本轮起点)
      与旧结果一致，则之后的旧结果直接沿用，不再重读重量。
    - 若旧末尾之后还有本次新增的序号，则从旧末尾状态继续处理新增部分。
    返回 (first_changed, resumed_at)。
    """
    first = changed[0]
    corrections = [s for s in changed if old_max is not None and s <= old_max]
    corrections_max = max(corrections) if corrections else None

    prev = conn.execute(
        "SELECT cumulative, round_start FROM run_state WHERE seq < ? "
        "ORDER BY seq DESC LIMIT 1", (first,)).fetchone()
    cum = prev[0] if prev else 0
    rs = prev[1] if prev else None

    states, events = [], []
    resumed_at = None
    # 游标逐行取数，break 后剩余旧称重不会被读取。
    cur = conn.execute(
        "SELECT seq, weight FROM weighings WHERE seq >= ? ORDER BY seq", (first,))
    for seq, weight in cur:
        if (corrections_max is not None
                and seq > corrections_max
                and seq <= old_max):
            old = conn.execute(
                "SELECT cumulative, round_start FROM run_state WHERE seq < ? "
                "ORDER BY seq DESC LIMIT 1", (seq,)).fetchone()
            if old is not None and old[0] == cum and old[1] == rs:
                resumed_at = seq   # 与旧结果汇合：旧尾段直接沿用
                break
        cum, rs = _accumulate(states, events, cum, rs, seq, weight)

    if resumed_at is None:
        conn.execute("DELETE FROM run_state WHERE seq >= ?", (first,))
        conn.execute("DELETE FROM events WHERE end_seq >= ?", (first,))
        _insert_results(conn, batch_id, states, events)
    else:
        conn.execute(
            "DELETE FROM run_state WHERE seq >= ? AND seq < ?", (first, resumed_at))
        conn.execute(
            "DELETE FROM events WHERE end_seq >= ? AND end_seq < ?", (first, resumed_at))
        _insert_results(conn, batch_id, states, events)
        # 旧尾段 [resumed_at, old_max] 原样保留；其后的新增序号从旧末尾状态继续。
        tail = conn.execute(
            "SELECT seq, weight FROM weighings WHERE seq > ? ORDER BY seq",
            (old_max,)).fetchall()
        if tail:
            end_state = conn.execute(
                "SELECT cumulative, round_start FROM run_state WHERE seq = ?",
                (old_max,)).fetchone()
            cum, rs = end_state[0], end_state[1]
            states2, events2 = [], []
            for seq, weight in tail:
                cum, rs = _accumulate(states2, events2, cum, rs, seq, weight)
            _insert_results(conn, batch_id, states2, events2)
    return first, resumed_at


def apply_upload(conn, rows):
    """在一个事务内应用一批称重并增量复算。调用方负责加锁与事务。"""
    old_max = conn.execute("SELECT MAX(seq) FROM weighings").fetchone()[0]
    changed = []
    additions = corrections = 0
    for seq, weight in rows:
        old = conn.execute(
            "SELECT weight FROM weighings WHERE seq = ?", (seq,)).fetchone()
        if old is None:
            if old_max is not None and seq <= old_max:
                raise ValueError(
                    f"新序号 {seq} 必须大于当前最大序号 {old_max}（新号只往后追加）")
            additions += 1
            changed.append(seq)
        elif old[0] != weight:
            corrections += 1
            changed.append(seq)
        # 同号同重：视为无改动
    cur = conn.execute(
        "INSERT INTO batches (rows, additions, corrections) VALUES (?,?,?)",
        (len(rows), additions, corrections))
    batch_id = cur.lastrowid
    conn.executemany(
        "INSERT INTO weighings (seq, weight) VALUES (?,?) "
        "ON CONFLICT(seq) DO UPDATE SET weight = excluded.weight", rows)
    first_changed = resumed_at = None
    paused_ledgers = []
    if changed:
        first_changed, resumed_at = _recompute(conn, sorted(changed), batch_id, old_max)
        conn.execute(
            "UPDATE batches SET first_changed = ?, resumed_at = ? WHERE id = ?",
            (first_changed, resumed_at, batch_id))
        # 称重更正照常生效；若让某份已发布台账的原配对不再是唯一解，
        # 同次操作（同一事务）内暂停该台账查袋。
        correction_seqs = [s for s in changed if old_max is not None and s <= old_max]
        paused_ledgers = pause_check_corrections(conn, correction_seqs)
    return {"batch_id": batch_id, "additions": additions, "corrections": corrections,
            "first_changed": first_changed, "resumed_at": resumed_at,
            "paused_ledgers": paused_ledgers}


# ---------------------------------------------------------------- 台账配对引擎

def analyze_matching(weighings, rows, verified):
    """计算台账配对分析（纯函数，便于测试）。

    weighings: [(seq, weight), ...] 按序号升序的区间称重；
    rows:      [(barcode, weight), ...] 按留样顺序的台账行；
    verified:  {barcode: seq} 已现场核实的固定配对（不再受克重差约束）。

    返回 dict：
      weighing_count / row_count  区间称重条数 / 台账行数
      exists     是否存在完整配对
      unique     完整配对是否唯一
      candidates 每行可配的序号列表（出现在某个完整配对中的序号）
      matching   一组代表性完整配对（每行取最小可行序号），配不齐时为 None
      pairs      唯一解 [(barcode, seq), ...]，非唯一或配不齐时为 None
    """
    m, n = len(weighings), len(rows)
    seqs = [s for s, _ in weighings]

    def compat(i, j):
        barcode, w = rows[i]
        fixed = verified.get(barcode)
        if fixed is not None:
            return seqs[j] == fixed
        return abs(w - weighings[j][1]) <= WEIGHT_TOLERANCE

    # L[i][j]：前 i 行台账能否配进前 j 条称重（保序、克重差约束）。
    L = [[False] * (m + 1) for _ in range(n + 1)]
    for j in range(m + 1):
        L[0][j] = True
    for i in range(1, n + 1):
        Li, Lprev = L[i], L[i - 1]
        for j in range(1, m + 1):
            Li[j] = Li[j - 1] or (Lprev[j - 1] and compat(i - 1, j - 1))

    # R[i][j]：第 i 行起的台账能否配进第 j 条起的称重。
    R = [[False] * (m + 1) for _ in range(n + 1)]
    for j in range(m + 1):
        R[n][j] = True
    for i in range(n - 1, -1, -1):
        Ri, Rnext = R[i], R[i + 1]
        for j in range(m - 1, -1, -1):
            Ri[j] = Ri[j + 1] or (Rnext[j + 1] and compat(i, j))

    exists = L[n][m]
    candidates = []
    for i in range(n):
        cands = [seqs[j] for j in range(m)
                 if compat(i, j) and L[i][j] and R[i + 1][j + 1]]
        candidates.append(cands)
    unique = exists and all(len(c) == 1 for c in candidates)

    matching = None
    if exists:
        # 贪心取每行最小可行序号：R 保证余下行仍能配完。
        matching = []
        prev = -1
        for i in range(n):
            j = prev + 1
            while j < m and not (compat(i, j) and R[i + 1][j + 1]):
                j += 1
            matching.append(seqs[j])
            prev = j

    return {
        "weighing_count": m,
        "row_count": n,
        "exists": exists,
        "unique": unique,
        "candidates": candidates,
        "matching": matching,
        "pairs": (list(zip([b for b, _ in rows], matching)) if unique else None),
    }


# ---------------------------------------------------------------- 台账数据操作

def fetch_ledger(conn, ledger_id):
    return conn.execute(
        "SELECT * FROM ledgers WHERE id = ?", (ledger_id,)).fetchone()


def analyze_ledger(conn, ledger):
    """按当前称重与核实记录分析一份台账。"""
    ws = conn.execute(
        "SELECT seq, weight FROM weighings WHERE seq BETWEEN ? AND ? ORDER BY seq",
        (ledger["start_seq"], ledger["end_seq"])).fetchall()
    rs = conn.execute(
        "SELECT barcode, weight FROM ledger_rows WHERE ledger_id = ? ORDER BY idx",
        (ledger["id"],)).fetchall()
    verified = {r["barcode"]: r["seq"] for r in conn.execute(
        "SELECT barcode, seq FROM verifications WHERE ledger_id = ?",
        (ledger["id"],)).fetchall()}
    result = analyze_matching(
        [(w["seq"], w["weight"]) for w in ws],
        [(r["barcode"], r["weight"]) for r in rs], verified)
    result["verified"] = verified
    return result


def active_pairs(conn, ledger_id):
    """当前生效发布的 [(barcode, seq), ...]；从未发布过返回 None。"""
    pub = conn.execute(
        "SELECT id FROM publications WHERE ledger_id = ? AND active = 1 "
        "ORDER BY id DESC LIMIT 1", (ledger_id,)).fetchone()
    if pub is None:
        return None
    rows = conn.execute(
        "SELECT barcode, seq FROM publication_pairs WHERE publication_id = ? "
        "ORDER BY idx", (pub["id"],)).fetchall()
    return [(r["barcode"], r["seq"]) for r in rows]


def pause_check_one(conn, ledger):
    """复核一份已发布台账：原配对不再是唯一解则暂停查袋。返回是否本次被暂停。"""
    analysis = analyze_ledger(conn, ledger)
    published = active_pairs(conn, ledger["id"])
    unique_pairs = analysis["pairs"]  # 非唯一或配不齐时为 None
    if (unique_pairs is not None and published is not None
            and [tuple(p) for p in unique_pairs]
            == [tuple(p) for p in published]):
        return False
    conn.execute(
        "UPDATE ledgers SET status = 'paused' WHERE id = ?", (ledger["id"],))
    return True


def pause_check_corrections(conn, changed_seqs):
    """称重更正后复核受影响的已发布台账，返回被暂停查袋的台账 id 列表。"""
    if not changed_seqs:
        return []
    paused = []
    ledgers = conn.execute(
        "SELECT * FROM ledgers WHERE status = 'published'").fetchall()
    for lg in ledgers:
        lo, hi = lg["start_seq"], lg["end_seq"]
        if any(lo <= s <= hi for s in changed_seqs):
            if pause_check_one(conn, lg):
                paused.append(lg["id"])
    return paused


def create_ledger(conn, start_seq, end_seq, rows):
    """建一份台账（同事务内调用）。数据问题抛 ValueError。"""
    if start_seq > end_seq:
        raise ValueError(f"起始序号 {start_seq} 不能大于截止序号 {end_seq}")
    for label, s in (("起始", start_seq), ("截止", end_seq)):
        if conn.execute(
                "SELECT 1 FROM weighings WHERE seq = ?", (s,)).fetchone() is None:
            raise ValueError(f"{label}序号 {s} 不存在于称重记录中")
    cnt = conn.execute(
        "SELECT COUNT(*) FROM weighings WHERE seq BETWEEN ? AND ?",
        (start_seq, end_seq)).fetchone()[0]
    if cnt > LEDGER_MAX_WEIGHINGS:
        raise ValueError(
            f"所选区间包含 {cnt} 条称重，超过 {LEDGER_MAX_WEIGHINGS} 条上限")
    cur = conn.execute(
        "INSERT INTO ledgers (start_seq, end_seq) VALUES (?,?)", (start_seq, end_seq))
    ledger_id = cur.lastrowid
    conn.executemany(
        "INSERT INTO ledger_rows (ledger_id, idx, barcode, weight) VALUES (?,?,?,?)",
        [(ledger_id, i, b, w) for i, (b, w) in enumerate(rows)])
    return ledger_id


def verify_pair(conn, ledger, barcode, seq):
    """登记现场核实（同事务内调用）。可行性不满足抛 ValueError，调用方回滚。

    返回是否因此暂停了该台账的查袋。
    """
    row = conn.execute(
        "SELECT 1 FROM ledger_rows WHERE ledger_id = ? AND barcode = ?",
        (ledger["id"], barcode)).fetchone()
    if row is None:
        raise ValueError(f"条码 {barcode} 不在台账 #{ledger['id']} 中")
    in_range = conn.execute(
        "SELECT 1 FROM weighings WHERE seq = ? AND seq BETWEEN ? AND ?",
        (seq, ledger["start_seq"], ledger["end_seq"])).fetchone()
    if in_range is None:
        raise ValueError(f"序号 {seq} 不在台账区间内的称重记录中")
    conn.execute(
        "INSERT INTO verifications (ledger_id, barcode, seq) VALUES (?,?,?) "
        "ON CONFLICT(ledger_id, barcode) DO UPDATE SET seq = excluded.seq, "
        "created_at = datetime('now')", (ledger["id"], barcode, seq))
    analysis = analyze_ledger(conn, ledger)
    if not analysis["exists"]:
        raise ValueError("该核实与已有记录一起配不齐整张台账，本次登记已退回")
    paused = ledger["status"] == "published" and pause_check_one(conn, ledger)
    return paused


def unverify_pair(conn, ledger, barcode):
    """撤销现场核实（同事务内调用）。返回是否因此暂停了查袋。"""
    cur = conn.execute(
        "DELETE FROM verifications WHERE ledger_id = ? AND barcode = ?",
        (ledger["id"], barcode))
    if cur.rowcount == 0:
        raise ValueError(f"条码 {barcode} 没有已登记的核实记录")
    paused = ledger["status"] == "published" and pause_check_one(conn, ledger)
    return paused


def publish_ledger(conn, ledger, submitted_pairs):
    """发布台账（同事务内调用）。条件不满足抛 ValueError。

    只有整份台账唯一配对才可发布，且提交配对必须与当前计算结果一致。
    重新发布时旧配对保留在 publications/publication_pairs 中供追查。
    """
    analysis = analyze_ledger(conn, ledger)
    if not analysis["exists"]:
        raise ValueError("配不齐整份台账，不能发布")
    if not analysis["unique"]:
        raise ValueError("台账配对不唯一，请先登记现场核实消除歧义")
    computed = [tuple(p) for p in analysis["pairs"]]
    if submitted_pairs is None:
        raise ValueError("缺少页面配对数据，请从台账页面重新提交")
    if [tuple(p) for p in submitted_pairs] != computed:
        raise ValueError("提交的配对与当前台账状态不一致（数据已变动），请刷新页面后重新发布")
    conn.execute(
        "UPDATE publications SET active = 0 WHERE ledger_id = ?", (ledger["id"],))
    cur = conn.execute(
        "INSERT INTO publications (ledger_id, active) VALUES (?, 1)", (ledger["id"],))
    pub_id = cur.lastrowid
    conn.executemany(
        "INSERT INTO publication_pairs (publication_id, idx, barcode, seq) "
        "VALUES (?,?,?,?)",
        [(pub_id, i, b, s) for i, (b, s) in enumerate(computed)])
    conn.execute(
        "UPDATE ledgers SET status = 'published' WHERE id = ?", (ledger["id"],))
    return computed


def lookup_barcode(conn, ledger, seq):
    """按序号查条码。返回 (barcode, None) 或 (None, 提示)。"""
    if ledger["status"] == "paused":
        return None, "查袋已暂停（原配对失效），需重新发布后才能查袋"
    if ledger["status"] != "published":
        return None, "台账尚未发布，发布后才能按序号查条码"
    if not (ledger["start_seq"] <= seq <= ledger["end_seq"]):
        return None, f"序号 {seq} 不在台账区间 {ledger['start_seq']}–{ledger['end_seq']} 内"
    for b, s in active_pairs(conn, ledger["id"]) or []:
        if s == seq:
            return b, None
    return None, f"序号 {seq} 未留样（没有对应条码）"


def encode_pairs(pairs):
    """配对 -> 表单隐藏域字符串（条码不含 , : ; 空白，建账时已校验）。"""
    return ";".join(f"{b},{s}" for b, s in pairs)


def decode_pairs(text):
    """表单隐藏域字符串 -> [(barcode, seq), ...]，非法抛 ValueError。"""
    pairs = []
    for chunk in text.strip().split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            b, s = chunk.rsplit(",", 1)
            pairs.append((b.strip(), int(s)))
        except ValueError:
            raise ValueError(f"配对数据格式错误：{chunk!r}")
    return pairs


LEDGER_STATUS_LABELS = {
    "draft": "草稿（未发布）",
    "published": "已发布（可查袋）",
    "paused": "已暂停（查袋停用）",
}


# ---------------------------------------------------------------- Flask 应用

def create_app(db_path=None):
    app = Flask(__name__)
    app.config["DB_PATH"] = (
        db_path or os.environ.get("FILLING_DB")
        or os.path.join(os.path.dirname(os.path.abspath(__file__)), "filling.db"))
    app.secret_key = os.environ.get("SECRET_KEY", "filling-line-dev-key")

    db_dir = os.path.dirname(app.config["DB_PATH"])
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    with sqlite3.connect(app.config["DB_PATH"]) as conn:
        conn.executescript(SCHEMA)

    def connect():
        conn = sqlite3.connect(app.config["DB_PATH"], timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def process_text(text):
        """解析并生效一批 CSV；成功返回结果 dict，数据问题抛 ValueError。"""
        rows = parse_csv(text)
        with _upload_lock:
            conn = connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                result = apply_upload(conn, rows)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def transact(fn):
        """加锁开一个写事务执行 fn(conn)；成功返回其结果，异常回滚并继续抛出。"""
        with _upload_lock:
            conn = connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                result = fn(conn)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    # ------------------------------------------------------------ 页面

    @app.get("/")
    def index():
        conn = connect()
        try:
            state = conn.execute(
                "SELECT seq, cumulative, round_start FROM run_state "
                "ORDER BY seq DESC LIMIT 1").fetchone()
            weighing_count = conn.execute(
                "SELECT COUNT(*) FROM weighings").fetchone()[0]
            event_count = conn.execute(
                "SELECT COUNT(*) FROM events").fetchone()[0]
            events = conn.execute(
                "SELECT e.id, e.start_seq, e.end_seq, e.grams, e.batch_id, b.applied_at "
                "FROM events e JOIN batches b ON b.id = e.batch_id "
                "ORDER BY e.end_seq DESC LIMIT 200").fetchall()
            recent = conn.execute(
                "SELECT seq, weight FROM weighings ORDER BY seq DESC LIMIT 50").fetchall()
            batches = conn.execute(
                "SELECT * FROM batches ORDER BY id DESC LIMIT 20").fetchall()
            ledgers = conn.execute(
                "SELECT l.*, (SELECT COUNT(*) FROM ledger_rows r "
                " WHERE r.ledger_id = l.id) AS row_count "
                "FROM ledgers l ORDER BY l.id DESC LIMIT 50").fetchall()
        finally:
            conn.close()
        return render_template(
            "index.html", state=state, weighing_count=weighing_count,
            event_count=event_count, events=events, recent=recent, batches=batches,
            ledgers=ledgers, status_labels=LEDGER_STATUS_LABELS,
            nominal=NOMINAL_GRAMS, trigger=TRIGGER_GRAMS,
            ledger_max=LEDGER_MAX_WEIGHINGS)

    @app.post("/upload")
    def upload():
        f = request.files.get("file")
        try:
            if f is not None and f.filename:
                text = f.read().decode("utf-8-sig")
            else:
                text = request.form.get("csv_text", "")
            result = process_text(text)
        except ValueError as exc:
            flash(f"上传未生效：{exc}", "error")
            return redirect(url_for("index"))
        except Exception:
            app.logger.exception("上传处理失败")
            flash("内部错误，本次上传未生效", "error")
            return redirect(url_for("index"))
        flash("批次 #{} 已生效：新增 {} 条、更正 {} 条{}".format(
            result["batch_id"], result["additions"], result["corrections"],
            _recompute_note(result)), "ok")
        return redirect(url_for("index"))

    @app.get("/ledger/<int:ledger_id>")
    def ledger_page(ledger_id):
        conn = connect()
        try:
            ledger = fetch_ledger(conn, ledger_id)
            if ledger is None:
                flash(f"台账 #{ledger_id} 不存在", "error")
                return redirect(url_for("index"))
            analysis = analyze_ledger(conn, ledger)
            rows = conn.execute(
                "SELECT idx, barcode, weight FROM ledger_rows "
                "WHERE ledger_id = ? ORDER BY idx", (ledger_id,)).fetchall()
            verifs = conn.execute(
                "SELECT barcode, seq, created_at FROM verifications "
                "WHERE ledger_id = ? ORDER BY created_at, barcode",
                (ledger_id,)).fetchall()
            pubs = conn.execute(
                "SELECT * FROM publications WHERE ledger_id = ? ORDER BY id DESC",
                (ledger_id,)).fetchall()
            pub_pairs = {}
            for p in pubs:
                pub_pairs[p["id"]] = conn.execute(
                    "SELECT idx, barcode, seq FROM publication_pairs "
                    "WHERE publication_id = ? ORDER BY idx", (p["id"],)).fetchall()
            lookup_seq = request.args.get("seq", type=int)
            lookup_result = lookup_error = None
            if lookup_seq is not None:
                lookup_result, lookup_error = lookup_barcode(conn, ledger, lookup_seq)
            elif request.args.get("seq"):
                lookup_error = "查询序号必须是整数"
        finally:
            conn.close()
        matching = analysis["matching"]
        view_rows = []
        for i, r in enumerate(rows):
            cands = analysis["candidates"][i]
            view_rows.append({
                "idx": r["idx"], "barcode": r["barcode"], "weight": r["weight"],
                "candidates": cands,
                "assigned": matching[i] if matching else None,
                "verified": analysis["verified"].get(r["barcode"]),
                "ambiguous": len(cands) > 1,
                "no_candidate": len(cands) == 0,
            })
        return render_template(
            "ledger.html", ledger=ledger, analysis=analysis, rows=view_rows,
            verifs=verifs, pubs=pubs, pub_pairs=pub_pairs,
            pairs_encoded=(encode_pairs(analysis["pairs"])
                           if analysis["pairs"] else ""),
            status_label=LEDGER_STATUS_LABELS[ledger["status"]],
            lookup_seq=lookup_seq, lookup_result=lookup_result,
            lookup_error=lookup_error, tolerance=WEIGHT_TOLERANCE)

    @app.post("/ledger/new")
    def ledger_new():
        try:
            start_seq = int(request.form.get("start_seq", ""))
            end_seq = int(request.form.get("end_seq", ""))
        except ValueError:
            flash("起始/截止序号必须是整数", "error")
            return redirect(url_for("index"))
        f = request.files.get("file")
        if f is not None and f.filename:
            text = f.read().decode("utf-8-sig")
        else:
            text = request.form.get("csv_text", "")
        try:
            rows = parse_ledger_csv(text)
            ledger_id = transact(
                lambda conn: create_ledger(conn, start_seq, end_seq, rows))
        except ValueError as exc:
            flash(f"台账未创建：{exc}", "error")
            return redirect(url_for("index"))
        except Exception:
            app.logger.exception("创建台账失败")
            flash("内部错误，台账未创建", "error")
            return redirect(url_for("index"))
        flash(f"台账 #{ledger_id} 已创建（区间 {start_seq}–{end_seq}，"
              f"{len(rows)} 行）", "ok")
        return redirect(url_for("ledger_page", ledger_id=ledger_id))

    @app.post("/ledger/<int:ledger_id>/verify")
    def ledger_verify(ledger_id):
        barcode = request.form.get("barcode", "").strip()
        try:
            seq = int(request.form.get("seq", ""))
        except ValueError:
            flash("核实序号必须是整数", "error")
            return redirect(url_for("ledger_page", ledger_id=ledger_id))
        try:
            def work(conn):
                ledger = fetch_ledger(conn, ledger_id)
                if ledger is None:
                    raise ValueError(f"台账 #{ledger_id} 不存在")
                return verify_pair(conn, ledger, barcode, seq)
            paused = transact(work)
        except ValueError as exc:
            flash(f"核实未登记：{exc}", "error")
            return redirect(url_for("ledger_page", ledger_id=ledger_id))
        except Exception:
            app.logger.exception("登记核实失败")
            flash("内部错误，核实未登记", "error")
            return redirect(url_for("ledger_page", ledger_id=ledger_id))
        note = "；该台账查袋已暂停，需重新发布" if paused else ""
        flash(f"已登记核实：条码 {barcode} ↔ 序号 {seq}{note}", "ok")
        return redirect(url_for("ledger_page", ledger_id=ledger_id))

    @app.post("/ledger/<int:ledger_id>/unverify")
    def ledger_unverify(ledger_id):
        barcode = request.form.get("barcode", "").strip()
        try:
            def work(conn):
                ledger = fetch_ledger(conn, ledger_id)
                if ledger is None:
                    raise ValueError(f"台账 #{ledger_id} 不存在")
                return unverify_pair(conn, ledger, barcode)
            paused = transact(work)
        except ValueError as exc:
            flash(f"核实未撤销：{exc}", "error")
            return redirect(url_for("ledger_page", ledger_id=ledger_id))
        except Exception:
            app.logger.exception("撤销核实失败")
            flash("内部错误，核实未撤销", "error")
            return redirect(url_for("ledger_page", ledger_id=ledger_id))
        note = "；该台账查袋已暂停，需重新发布" if paused else ""
        flash(f"已撤销核实：条码 {barcode}{note}", "ok")
        return redirect(url_for("ledger_page", ledger_id=ledger_id))

    @app.post("/ledger/<int:ledger_id>/publish")
    def ledger_publish(ledger_id):
        try:
            submitted = decode_pairs(request.form.get("pairs", ""))
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("ledger_page", ledger_id=ledger_id))
        try:
            def work(conn):
                ledger = fetch_ledger(conn, ledger_id)
                if ledger is None:
                    raise ValueError(f"台账 #{ledger_id} 不存在")
                return publish_ledger(conn, ledger, submitted)
            pairs = transact(work)
        except ValueError as exc:
            flash(f"发布未生效：{exc}", "error")
            return redirect(url_for("ledger_page", ledger_id=ledger_id))
        except Exception:
            app.logger.exception("发布台账失败")
            flash("内部错误，发布未生效", "error")
            return redirect(url_for("ledger_page", ledger_id=ledger_id))
        flash(f"台账 #{ledger_id} 已发布（{len(pairs)} 对），可按序号查条码", "ok")
        return redirect(url_for("ledger_page", ledger_id=ledger_id))

    # ------------------------------------------------------------ API

    @app.post("/api/upload")
    def api_upload():
        text = request.get_data(as_text=True)
        try:
            result = process_text(text)
        except ValueError as exc:
            return jsonify(ok=False, error=str(exc)), 400
        except Exception:
            app.logger.exception("上传处理失败")
            return jsonify(ok=False, error="内部错误，本次上传未生效"), 500
        return jsonify(ok=True, **result)

    @app.get("/api/state")
    def api_state():
        conn = connect()
        try:
            state = conn.execute(
                "SELECT seq, cumulative, round_start FROM run_state "
                "ORDER BY seq DESC LIMIT 1").fetchone()
            weighing_count = conn.execute(
                "SELECT COUNT(*) FROM weighings").fetchone()[0]
            event_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        finally:
            conn.close()
        return jsonify(
            last_seq=state["seq"] if state else None,
            cumulative=state["cumulative"] if state else 0,
            round_start=state["round_start"] if state else None,
            weighing_count=weighing_count, event_count=event_count)

    @app.get("/api/events")
    def api_events():
        conn = connect()
        try:
            rows = conn.execute(
                "SELECT id, batch_id, start_seq, end_seq, grams FROM events "
                "ORDER BY end_seq").fetchall()
        finally:
            conn.close()
        return jsonify([dict(r) for r in rows])

    @app.get("/api/weighings")
    def api_weighings():
        limit = min(int(request.args.get("limit", 200)), 10000)
        conn = connect()
        try:
            rows = conn.execute(
                "SELECT seq, weight FROM weighings ORDER BY seq LIMIT ?", (limit,)
            ).fetchall()
        finally:
            conn.close()
        return jsonify([dict(r) for r in rows])

    @app.post("/api/ledgers")
    def api_ledger_create():
        data = request.get_json(silent=True) or {}
        try:
            start_seq = int(data.get("start_seq"))
            end_seq = int(data.get("end_seq"))
        except (TypeError, ValueError):
            return jsonify(ok=False, error="start_seq/end_seq 必须是整数"), 400
        try:
            rows = parse_ledger_csv(data.get("csv", ""))
            ledger_id = transact(
                lambda conn: create_ledger(conn, start_seq, end_seq, rows))
        except ValueError as exc:
            return jsonify(ok=False, error=str(exc)), 400
        except Exception:
            app.logger.exception("创建台账失败")
            return jsonify(ok=False, error="内部错误，台账未创建"), 500
        return jsonify(ok=True, ledger_id=ledger_id, rows=len(rows))

    def _ledger_payload(conn, ledger):
        analysis = analyze_ledger(conn, ledger)
        rows = conn.execute(
            "SELECT idx, barcode, weight FROM ledger_rows "
            "WHERE ledger_id = ? ORDER BY idx", (ledger["id"],)).fetchall()
        verifs = conn.execute(
            "SELECT barcode, seq, created_at FROM verifications "
            "WHERE ledger_id = ? ORDER BY created_at, barcode",
            (ledger["id"],)).fetchall()
        pubs = conn.execute(
            "SELECT * FROM publications WHERE ledger_id = ? ORDER BY id DESC",
            (ledger["id"],)).fetchall()
        publications = []
        for p in pubs:
            pairs = conn.execute(
                "SELECT barcode, seq FROM publication_pairs "
                "WHERE publication_id = ? ORDER BY idx", (p["id"],)).fetchall()
            publications.append({
                "id": p["id"], "published_at": p["published_at"],
                "active": bool(p["active"]),
                "pairs": [[pp["barcode"], pp["seq"]] for pp in pairs]})
        matching = analysis["matching"]
        return {
            "ok": True,
            "id": ledger["id"],
            "start_seq": ledger["start_seq"],
            "end_seq": ledger["end_seq"],
            "status": ledger["status"],
            "created_at": ledger["created_at"],
            "weighing_count": analysis["weighing_count"],
            "row_count": analysis["row_count"],
            "exists": analysis["exists"],
            "unique": analysis["unique"],
            "rows": [{
                "idx": r["idx"], "barcode": r["barcode"], "weight": r["weight"],
                "candidates": analysis["candidates"][r["idx"]],
                "assigned": (matching[r["idx"]] if matching else None),
                "verified": analysis["verified"].get(r["barcode"]),
                "ambiguous": len(analysis["candidates"][r["idx"]]) > 1,
            } for r in rows],
            "pairs": ([list(p) for p in analysis["pairs"]]
                      if analysis["pairs"] else None),
            "verifications": [dict(v) for v in verifs],
            "publications": publications,
        }

    @app.get("/api/ledgers/<int:ledger_id>")
    def api_ledger(ledger_id):
        conn = connect()
        try:
            ledger = fetch_ledger(conn, ledger_id)
            if ledger is None:
                return jsonify(ok=False, error=f"台账 #{ledger_id} 不存在"), 404
            payload = _ledger_payload(conn, ledger)
        finally:
            conn.close()
        return jsonify(payload)

    @app.post("/api/ledgers/<int:ledger_id>/verify")
    def api_ledger_verify(ledger_id):
        data = request.get_json(silent=True) or {}
        barcode = str(data.get("barcode", "")).strip()
        try:
            seq = int(data.get("seq"))
        except (TypeError, ValueError):
            return jsonify(ok=False, error="seq 必须是整数"), 400
        try:
            def work(conn):
                ledger = fetch_ledger(conn, ledger_id)
                if ledger is None:
                    raise LookupError(ledger_id)
                return verify_pair(conn, ledger, barcode, seq)
            paused = transact(work)
        except LookupError:
            return jsonify(ok=False, error=f"台账 #{ledger_id} 不存在"), 404
        except ValueError as exc:
            return jsonify(ok=False, error=str(exc)), 400
        except Exception:
            app.logger.exception("登记核实失败")
            return jsonify(ok=False, error="内部错误，核实未登记"), 500
        return jsonify(ok=True, paused=bool(paused))

    @app.post("/api/ledgers/<int:ledger_id>/unverify")
    def api_ledger_unverify(ledger_id):
        data = request.get_json(silent=True) or {}
        barcode = str(data.get("barcode", "")).strip()
        try:
            def work(conn):
                ledger = fetch_ledger(conn, ledger_id)
                if ledger is None:
                    raise LookupError(ledger_id)
                return unverify_pair(conn, ledger, barcode)
            paused = transact(work)
        except LookupError:
            return jsonify(ok=False, error=f"台账 #{ledger_id} 不存在"), 404
        except ValueError as exc:
            return jsonify(ok=False, error=str(exc)), 400
        except Exception:
            app.logger.exception("撤销核实失败")
            return jsonify(ok=False, error="内部错误，核实未撤销"), 500
        return jsonify(ok=True, paused=bool(paused))

    @app.post("/api/ledgers/<int:ledger_id>/publish")
    def api_ledger_publish(ledger_id):
        data = request.get_json(silent=True)
        if data is None or "pairs" not in data:
            return jsonify(ok=False, error="缺少 pairs（页面配对数据）"), 400
        try:
            submitted = [(str(b), int(s)) for b, s in data["pairs"]]
        except (TypeError, ValueError):
            return jsonify(ok=False, error="pairs 格式应为 [[条码, 序号], ...]"), 400
        try:
            def work(conn):
                ledger = fetch_ledger(conn, ledger_id)
                if ledger is None:
                    raise LookupError(ledger_id)
                return publish_ledger(conn, ledger, submitted)
            pairs = transact(work)
        except LookupError:
            return jsonify(ok=False, error=f"台账 #{ledger_id} 不存在"), 404
        except ValueError as exc:
            return jsonify(ok=False, error=str(exc)), 400
        except Exception:
            app.logger.exception("发布台账失败")
            return jsonify(ok=False, error="内部错误，发布未生效"), 500
        return jsonify(ok=True, pairs=[list(p) for p in pairs])

    @app.get("/api/ledgers/<int:ledger_id>/lookup")
    def api_ledger_lookup(ledger_id):
        seq = request.args.get("seq", type=int)
        if seq is None:
            return jsonify(ok=False, error="缺少整数参数 seq"), 400
        conn = connect()
        try:
            ledger = fetch_ledger(conn, ledger_id)
            if ledger is None:
                return jsonify(ok=False, error=f"台账 #{ledger_id} 不存在"), 404
            barcode, error = lookup_barcode(conn, ledger, seq)
            status = ledger["status"]
        finally:
            conn.close()
        if error:
            code = 409 if status != "published" else 404
            return jsonify(ok=False, error=error, status=status), code
        return jsonify(ok=True, seq=seq, barcode=barcode)

    @app.get("/healthz")
    def healthz():
        return "ok"

    return app


def _recompute_note(result):
    if result["first_changed"] is None:
        return "；无实际改动，未触发复算"
    note = f"；复算自序号 {result['first_changed']} 起"
    if result["resumed_at"] is not None:
        note += f"，自 {result['resumed_at']} 起沿用旧结果"
    else:
        note += "，已复算至末尾"
    if result["paused_ledgers"]:
        note += "；台账 " + "、".join(
            f"#{i}" for i in result["paused_ledgers"]) + " 查袋已暂停"
    return note


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), threaded=True)
