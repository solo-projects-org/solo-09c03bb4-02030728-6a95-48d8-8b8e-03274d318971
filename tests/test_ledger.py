"""留样台账测试：配对分析、现场核实消歧、发布与查袋、更正/撤销后的暂停与重新发布。"""

import os
import random
import sqlite3
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("FILLING_DB",
                      os.path.join(tempfile.gettempdir(), "filling_import_default.db"))

from app import (create_app, analyze_matching, parse_ledger_csv,  # noqa: E402
                 LEDGER_MAX_ROWS, LEDGER_MAX_WEIGHINGS)


@pytest.fixture()
def app(tmp_path):
    return create_app(str(tmp_path / "test.db"))


@pytest.fixture()
def client(app):
    return app.test_client()


# ------------------------------------------------------------------ 工具

def upload(client, rows):
    body = "\n".join(f"{s},{w}" for s, w in rows)
    r = client.post("/api/upload", data=body, content_type="text/csv")
    assert r.status_code == 200
    return r.get_json()


def make_ledger(client, start, end, csv_text):
    return client.post("/api/ledgers",
                       json={"start_seq": start, "end_seq": end, "csv": csv_text})


def new_ledger(client, start, end, csv_text):
    r = make_ledger(client, start, end, csv_text)
    assert r.status_code == 200, r.get_json()
    return r.get_json()["ledger_id"]


def analysis(client, lid):
    r = client.get(f"/api/ledgers/{lid}")
    assert r.status_code == 200
    return r.get_json()


def verify(client, lid, barcode, seq):
    return client.post(f"/api/ledgers/{lid}/verify",
                       json={"barcode": barcode, "seq": seq})


def unverify(client, lid, barcode):
    return client.post(f"/api/ledgers/{lid}/unverify", json={"barcode": barcode})


def publish(client, lid, pairs):
    return client.post(f"/api/ledgers/{lid}/publish", json={"pairs": pairs})


def publish_shown(client, lid):
    """按页面所见（API 返回的唯一配对）发布。"""
    body = analysis(client, lid)
    assert body["unique"]
    return publish(client, lid, body["pairs"])


def lookup(client, lid, seq):
    return client.get(f"/api/ledgers/{lid}/lookup?seq={seq}")


def db(app, sql, args=()):
    conn = sqlite3.connect(app.config["DB_PATH"])
    try:
        return conn.execute(sql, args).fetchall()
    finally:
        conn.close()


def ledger_snapshot(app):
    return (
        db(app, "SELECT id, start_seq, end_seq, status FROM ledgers ORDER BY id"),
        db(app, "SELECT ledger_id, idx, barcode, weight FROM ledger_rows "
                "ORDER BY ledger_id, idx"),
        db(app, "SELECT ledger_id, barcode, seq FROM verifications "
                "ORDER BY ledger_id, barcode"),
        db(app, "SELECT id, ledger_id, active FROM publications ORDER BY id"),
        db(app, "SELECT publication_id, idx, barcode, seq FROM publication_pairs "
                "ORDER BY publication_id, idx"),
    )


# ------------------------------------------------------------------ 建账校验

def test_create_ledger_range_validation(client, app):
    upload(client, [(i, 500) for i in range(1, 6)])
    # 截止序号不存在
    r = make_ledger(client, 1, 6, "A,500")
    assert r.status_code == 400 and "不存在" in r.get_json()["error"]
    # 起始 > 截止
    r = make_ledger(client, 4, 2, "A,500")
    assert r.status_code == 400
    # 区间为空（端点不存在）
    r = make_ledger(client, 0, 3, "A,500")
    assert r.status_code == 400
    assert db(app, "SELECT COUNT(*) FROM ledgers") == [(0,)]


def test_create_ledger_weighing_cap(client, app):
    upload(client, [(i, 500) for i in range(1, LEDGER_MAX_WEIGHINGS + 2)])
    # 恰好 200 条称重的区间可以建账
    r = make_ledger(client, 1, LEDGER_MAX_WEIGHINGS, "A,500")
    assert r.status_code == 200
    # 201 条称重的区间拒绝
    r = make_ledger(client, 1, LEDGER_MAX_WEIGHINGS + 1, "A,500")
    assert r.status_code == 400 and "上限" in r.get_json()["error"]
    assert db(app, "SELECT COUNT(*) FROM ledgers") == [(1,)]


def test_create_ledger_csv_validation(client, app):
    upload(client, [(i, 500) for i in range(1, 4)])
    # 条码重复
    r = make_ledger(client, 1, 3, "A,500\nA,501")
    assert r.status_code == 400 and "重复" in r.get_json()["error"]
    # 克重非整数
    r = make_ledger(client, 1, 3, "A,abc")
    assert r.status_code == 400
    # 空 CSV
    r = make_ledger(client, 1, 3, "条码,克重")
    assert r.status_code == 400
    # 行数超过上限
    big = "\n".join(f"B{i},500" for i in range(LEDGER_MAX_ROWS + 1))
    r = make_ledger(client, 1, 3, big)
    assert r.status_code == 400 and "最多" in r.get_json()["error"]
    # 恰好上限行数可建（行数多于区间称重 -> 配不齐，但允许建账后给提示）
    big = "\n".join(f"B{i},500" for i in range(LEDGER_MAX_ROWS))
    r = make_ledger(client, 1, 3, big)
    assert r.status_code == 200
    lid = r.get_json()["ledger_id"]
    assert analysis(client, lid)["exists"] is False
    assert db(app, "SELECT COUNT(*) FROM ledgers") == [(1,)]


def test_parse_ledger_csv_units():
    assert parse_ledger_csv("条码,克重\nA,500\nB,499\n") == [("A", 500), ("B", 499)]
    assert parse_ledger_csv("A,500\n\nB,501\n") == [("A", 500), ("B", 501)]
    with pytest.raises(ValueError):
        parse_ledger_csv("A,500\nA,501")       # 条码重复
    with pytest.raises(ValueError):
        parse_ledger_csv("A,500\nB")           # 列数不对
    with pytest.raises(ValueError):
        parse_ledger_csv("A,-1")               # 负克重
    with pytest.raises(ValueError):
        parse_ledger_csv("")                   # 空
    with pytest.raises(ValueError):
        parse_ledger_csv("A B,500")            # 条码含空白


# ------------------------------------------------------------------ 配对分析

def test_matching_unique_with_skips_and_tolerance(client, app):
    # 称重 1:500 2:700 3:500 4:700 5:500；台账 (A,500),(B,500)
    upload(client, [(1, 500), (2, 700), (3, 500), (4, 700), (5, 500)])
    lid = new_ledger(client, 1, 5, "A,500\nB,500")
    body = analysis(client, lid)
    assert body["exists"] is True and body["unique"] is False
    rows = {r["barcode"]: r for r in body["rows"]}
    # A=5 时 B 无处可配，所以 A 的候选只有 1、3
    assert rows["A"]["candidates"] == [1, 3]
    assert rows["B"]["candidates"] == [3, 5]
    assert rows["A"]["ambiguous"] and rows["B"]["ambiguous"]


def test_tolerance_boundary(client, app):
    upload(client, [(1, 499), (2, 501), (3, 502)])
    lid = new_ledger(client, 1, 3, "A,500")
    rows = analysis(client, lid)["rows"]
    # 差 1 克可配（1、2），差 2 克不可配（3）
    assert rows[0]["candidates"] == [1, 2]


def test_unmatchable_prompt_and_publish_blocked(client, app):
    upload(client, [(1, 500), (2, 500)])
    lid = new_ledger(client, 1, 2, "A,600")  # 差 100 克，谁都配不上
    body = analysis(client, lid)
    assert body["exists"] is False and body["unique"] is False
    assert body["rows"][0]["candidates"] == []
    r = publish(client, lid, [])
    assert r.status_code == 400 and "配不齐" in r.get_json()["error"]
    # 页面包含提示
    page = client.get(f"/ledger/{lid}").get_data(as_text=True)
    assert "配不齐整份台账" in page


def test_ambiguous_page_marks(client, app):
    upload(client, [(i, 500) for i in range(1, 4)])
    lid = new_ledger(client, 1, 3, "A,500\nB,500")
    page = client.get(f"/ledger/{lid}").get_data(as_text=True)
    assert "多种完整配对" in page and "歧义" in page


# ------------------------------------------------------------------ 现场核实

def test_verify_disambiguates_then_publish_and_lookup(client, app):
    upload(client, [(i, 500) for i in range(1, 6)])
    lid = new_ledger(client, 1, 5, "A,500\nB,500")
    assert analysis(client, lid)["unique"] is False
    # 登记核实消歧：A↔2 后 B 仍有多种选择，仍不唯一
    r = verify(client, lid, "A", 2)
    assert r.status_code == 200 and r.get_json()["paused"] is False
    body = analysis(client, lid)
    assert body["exists"] is True and body["unique"] is False
    assert body["rows"][0]["verified"] == 2
    # 再核实 B↔4 后唯一，可发布
    assert verify(client, lid, "B", 4).status_code == 200
    body = analysis(client, lid)
    assert body["unique"] is True
    assert body["pairs"] == [["A", 2], ["B", 4]]
    r = publish_shown(client, lid)
    assert r.status_code == 200
    assert analysis(client, lid)["status"] == "published"
    # 发布后可按序号查条码
    assert lookup(client, lid, 2).get_json()["barcode"] == "A"
    assert lookup(client, lid, 4).get_json()["barcode"] == "B"
    # 区间内未留样的序号
    r = lookup(client, lid, 3)
    assert r.status_code == 404 and "未留样" in r.get_json()["error"]


def test_verify_rejected_when_breaks_whole_matching(client, app):
    upload(client, [(1, 500), (2, 500)])
    lid = new_ledger(client, 1, 2, "A,500\nB,500")
    # A↔2 后 B 必须排在 A 之后，没有可用序号 -> 配不齐 -> 退回
    r = verify(client, lid, "A", 2)
    assert r.status_code == 400 and "配不齐" in r.get_json()["error"]
    assert analysis(client, lid)["verifications"] == []  # 登记被退回，状态不变
    # A↔1 可行；随后 B↔1 与已有记录配不齐（序号须递增），也退回
    assert verify(client, lid, "A", 1).status_code == 200
    r = verify(client, lid, "B", 1)
    assert r.status_code == 400
    assert [v["barcode"] for v in analysis(client, lid)["verifications"]] == ["A"]
    # B↔2 可行
    assert verify(client, lid, "B", 2).status_code == 200
    assert analysis(client, lid)["unique"] is True


def test_verify_unknown_barcode_and_seq(client, app):
    upload(client, [(1, 500), (2, 500)])
    lid = new_ledger(client, 1, 2, "A,500")
    assert verify(client, lid, "NOPE", 1).status_code == 400
    assert verify(client, lid, "A", 9).status_code == 400
    assert analysis(client, lid)["verifications"] == []


def test_verify_update_same_barcode(client, app):
    upload(client, [(1, 500), (2, 500)])
    lid = new_ledger(client, 1, 2, "A,500")
    assert verify(client, lid, "A", 1).status_code == 200
    assert verify(client, lid, "A", 2).status_code == 200  # 改登记
    verifs = analysis(client, lid)["verifications"]
    assert len(verifs) == 1 and verifs[0]["seq"] == 2


def test_unverify(client, app):
    upload(client, [(1, 500), (2, 500)])
    lid = new_ledger(client, 1, 2, "A,500")
    assert verify(client, lid, "A", 1).status_code == 200
    assert unverify(client, lid, "A").status_code == 200
    assert analysis(client, lid)["verifications"] == []
    # 撤销不存在的记录
    assert unverify(client, lid, "A").status_code == 400


# ------------------------------------------------------------------ 发布

def test_publish_requires_unique_and_consistent_pairs(client, app):
    upload(client, [(1, 500), (2, 600), (3, 700)])
    # 唯一配对：A->1, B->2
    lid = new_ledger(client, 1, 3, "A,500\nB,600")
    # 提交的配对与页面不一致 -> 拒绝
    r = publish(client, lid, [["A", 1], ["B", 3]])
    assert r.status_code == 400 and "不一致" in r.get_json()["error"]
    assert analysis(client, lid)["status"] == "draft"
    # 与页面一致 -> 发布
    r = publish_shown(client, lid)
    assert r.status_code == 200
    assert r.get_json()["pairs"] == [["A", 1], ["B", 2]]
    assert analysis(client, lid)["status"] == "published"


def test_publish_blocked_when_ambiguous(client, app):
    upload(client, [(1, 500), (2, 500)])
    lid = new_ledger(client, 1, 2, "A,500")
    r = publish(client, lid, [["A", 1]])
    assert r.status_code == 400 and "不唯一" in r.get_json()["error"]
    assert analysis(client, lid)["status"] == "draft"


def test_lookup_requires_publish(client, app):
    upload(client, [(1, 500)])
    lid = new_ledger(client, 1, 1, "A,500")
    r = lookup(client, lid, 1)
    assert r.status_code == 409 and "尚未发布" in r.get_json()["error"]


# ------------------------------------------------------------------ 更正后停查 / 重新发布

def _published_AB(client):
    """建一份唯一配对的已发布台账：A->3, B->4。"""
    upload(client, [(1, 500), (2, 500), (3, 600), (4, 500)])
    lid = new_ledger(client, 1, 4, "A,600\nB,500")
    assert publish_shown(client, lid).status_code == 200
    assert analysis(client, lid)["pairs"] == [["A", 3], ["B", 4]]
    return lid


def test_correction_breaking_uniqueness_pauses_lookup(client, app):
    lid = _published_AB(client)
    assert lookup(client, lid, 3).get_json()["barcode"] == "A"
    # 更正序号 2 为 600：A 可配 2 或 3，原配对不再是唯一解 -> 同次操作暂停查袋
    body = upload(client, [(2, 600)])
    assert body["paused_ledgers"] == [lid]
    assert analysis(client, lid)["status"] == "paused"
    r = lookup(client, lid, 3)
    assert r.status_code == 409 and "暂停" in r.get_json()["error"]
    # 旧配对保留供追查
    pubs = analysis(client, lid)["publications"]
    assert len(pubs) == 1 and pubs[0]["pairs"] == [["A", 3], ["B", 4]]
    # 不唯一时不能重新发布
    r = publish(client, lid, [["A", 3], ["B", 4]])
    assert r.status_code == 400
    # 现场核实消歧后重新发布，恢复查袋
    assert verify(client, lid, "A", 2).status_code == 200
    body = analysis(client, lid)
    assert body["unique"] is True and body["pairs"] == [["A", 2], ["B", 4]]
    assert publish_shown(client, lid).status_code == 200
    assert analysis(client, lid)["status"] == "published"
    assert lookup(client, lid, 2).get_json()["barcode"] == "A"
    assert lookup(client, lid, 4).get_json()["barcode"] == "B"
    assert lookup(client, lid, 3).status_code == 404  # 序号 3 在新配对中未留样
    # 旧配对仍保留在发布记录中
    pubs = analysis(client, lid)["publications"]
    assert len(pubs) == 2
    assert pubs[1]["pairs"] == [["A", 3], ["B", 4]] and pubs[1]["active"] is False
    assert pubs[0]["pairs"] == [["A", 2], ["B", 4]] and pubs[0]["active"] is True


def test_correction_invalidating_pairs_pauses(client, app):
    lid = _published_AB(client)
    # 更正序号 3 为 500：A(600) 无处可配 -> 配不齐 -> 暂停
    body = upload(client, [(3, 500)])
    assert body["paused_ledgers"] == [lid]
    body = analysis(client, lid)
    assert body["status"] == "paused" and body["exists"] is False
    assert lookup(client, lid, 3).status_code == 409


def test_correction_keeping_uniqueness_stays_published(client, app):
    lid = _published_AB(client)
    # 更正序号 1：500 -> 501，不影响 A->3、B->4 的唯一性
    body = upload(client, [(1, 501)])
    assert body["paused_ledgers"] == []
    assert analysis(client, lid)["status"] == "published"
    assert lookup(client, lid, 3).get_json()["barcode"] == "A"


def test_correction_outside_range_stays_published(client, app):
    lid = _published_AB(client)
    upload(client, [(5, 500)])  # 追加区间外序号
    body = upload(client, [(5, 700)])  # 更正区间外序号
    assert body["paused_ledgers"] == []
    assert analysis(client, lid)["status"] == "published"


def test_unverify_breaking_uniqueness_pauses(client, app):
    upload(client, [(i, 500) for i in range(1, 4)])
    lid = new_ledger(client, 1, 3, "A,500\nB,500")
    assert verify(client, lid, "A", 1).status_code == 200
    assert verify(client, lid, "B", 2).status_code == 200
    assert publish_shown(client, lid).status_code == 200
    assert lookup(client, lid, 2).get_json()["barcode"] == "B"
    # 撤销 B 的核实：B 可配 2 或 3 -> 原配对不再是唯一解 -> 同次操作暂停
    r = unverify(client, lid, "B")
    assert r.status_code == 200 and r.get_json()["paused"] is True
    assert analysis(client, lid)["status"] == "paused"
    assert lookup(client, lid, 2).status_code == 409
    # 旧配对保留；重新核实消歧并重新发布后恢复
    pubs = analysis(client, lid)["publications"]
    assert pubs[0]["pairs"] == [["A", 1], ["B", 2]]
    assert verify(client, lid, "B", 3).status_code == 200
    assert publish_shown(client, lid).status_code == 200
    assert lookup(client, lid, 3).get_json()["barcode"] == "B"


def test_verify_contradicting_published_pairs_pauses(client, app):
    lid = _published_AB(client)
    # 登记与原配对矛盾的核实（A↔2 而非 A↔3）：仍有完整配对，登记成功，
    # 但原配对失效 -> 同次操作暂停查袋
    r = verify(client, lid, "A", 2)
    assert r.status_code == 200 and r.get_json()["paused"] is True
    assert analysis(client, lid)["status"] == "paused"
    assert lookup(client, lid, 3).status_code == 409


def test_verify_consistent_with_published_pairs_keeps_lookup(client, app):
    upload(client, [(i, 500) for i in range(1, 4)])
    lid = new_ledger(client, 1, 3, "A,500\nB,500")
    assert verify(client, lid, "A", 1).status_code == 200
    assert verify(client, lid, "B", 2).status_code == 200
    assert publish_shown(client, lid).status_code == 200
    # 重复登记同一核实（与发布配对一致）：不暂停
    r = verify(client, lid, "A", 1)
    assert r.status_code == 200 and r.get_json()["paused"] is False
    assert analysis(client, lid)["status"] == "published"
    assert lookup(client, lid, 2).get_json()["barcode"] == "B"


# ------------------------------------------------------------------ 失败保留状态 / 重启持久化

def test_failed_operations_keep_state(client, app):
    lid = _published_AB(client)
    before = ledger_snapshot(app)
    # 失败的发布（不一致的配对）
    assert publish(client, lid, [["A", 3], ["B", 1]]).status_code == 400
    # 失败的核实（配不齐）
    assert verify(client, lid, "B", 3).status_code == 400
    # 失败的建账（区间不存在）
    assert make_ledger(client, 1, 99, "X,500").status_code == 400
    # 失败的撤销
    assert unverify(client, lid, "ZZZ").status_code == 400
    assert ledger_snapshot(app) == before
    assert lookup(client, lid, 3).get_json()["barcode"] == "A"


def test_restart_persistence(tmp_path):
    dbfile = str(tmp_path / "restart.db")
    c1 = create_app(dbfile).test_client()
    upload(c1, [(1, 500), (2, 500), (3, 600), (4, 500)])
    lid = new_ledger(c1, 1, 4, "A,600\nB,500")
    assert publish_shown(c1, lid).status_code == 200
    upload(c1, [(2, 600)])  # 更正 -> 暂停查袋
    assert analysis(c1, lid)["status"] == "paused"
    # 模拟重启：同一数据库文件新建应用实例
    c2 = create_app(dbfile).test_client()
    body = analysis(c2, lid)
    assert body["status"] == "paused"
    assert body["publications"][0]["pairs"] == [["A", 3], ["B", 4]]
    assert lookup(c2, lid, 3).status_code == 409
    # 重启后仍可继续操作：核实消歧并重新发布
    assert verify(c2, lid, "A", 2).status_code == 200
    assert publish_shown(c2, lid).status_code == 200
    assert lookup(c2, lid, 2).get_json()["barcode"] == "A"


# ------------------------------------------------------------------ 页面冒烟

def test_html_flow(client, app):
    upload(client, [(i, 500) for i in range(1, 4)])
    # 表单建账
    r = client.post("/ledger/new", data={
        "start_seq": "1", "end_seq": "3", "csv_text": "A,500\nB,500"},
        follow_redirects=True)
    assert r.status_code == 200
    lid = db(app, "SELECT id FROM ledgers")[0][0]
    page = client.get(f"/ledger/{lid}").get_data(as_text=True)
    assert "配对分析" in page and "歧义" in page
    # 表单核实消歧
    r = client.post(f"/ledger/{lid}/verify", data={"barcode": "A", "seq": "1"},
                    follow_redirects=True)
    assert "已登记核实" in r.get_data(as_text=True)
    r = client.post(f"/ledger/{lid}/verify", data={"barcode": "B", "seq": "2"},
                    follow_redirects=True)
    assert "已登记核实" in r.get_data(as_text=True)
    # 表单发布（配对取自 API 所见）
    pairs = analysis(client, lid)["pairs"]
    encoded = ";".join(f"{b},{s}" for b, s in pairs)
    r = client.post(f"/ledger/{lid}/publish", data={"pairs": encoded},
                    follow_redirects=True)
    assert "已发布" in r.get_data(as_text=True)
    # 台账页按序号查条码
    page = client.get(f"/ledger/{lid}?seq=2").get_data(as_text=True)
    assert "留样袋条码" in page and "B" in page
    # 首页台账列表
    page = client.get("/").get_data(as_text=True)
    assert "留样台账" in page and "已发布" in page


def test_index_shows_ledgers(client, app):
    upload(client, [(1, 500)])
    new_ledger(client, 1, 1, "A,500")
    page = client.get("/").get_data(as_text=True)
    assert "留样台账" in page and "草稿" in page


# ------------------------------------------------------------------ 配对引擎随机对照

def _brute_force(weighings, rows, verified):
    """枚举全部完整配对，返回 {(seq_i0, seq_i1, ...), ...}。"""
    m, n = len(weighings), len(rows)
    seqs = [s for s, _ in weighings]
    results = set()

    def compat(i, j):
        barcode, w = rows[i]
        fixed = verified.get(barcode)
        if fixed is not None:
            return seqs[j] == fixed
        return abs(w - weighings[j][1]) <= 1

    def rec(i, prev, acc):
        if i == n:
            results.add(tuple(acc))
            return
        for j in range(prev + 1, m):
            if compat(i, j):
                acc.append(seqs[j])
                rec(i + 1, j, acc)
                acc.pop()

    rec(0, -1, [])
    return results


def test_analyze_matching_against_brute_force():
    rng = random.Random(20261121)
    for _ in range(400):
        m = rng.randint(1, 8)
        n = rng.randint(1, 4)
        seqs = sorted(rng.sample(range(1, 30), m))
        weighings = [(s, rng.randint(495, 505)) for s in seqs]
        rows = [(f"B{i}", rng.randint(495, 505)) for i in range(n)]
        verified = {}
        for i in range(n):
            if rng.random() < 0.3:
                verified[f"B{i}"] = rng.choice(seqs)
        got = analyze_matching(weighings, rows, verified)
        all_matchings = _brute_force(weighings, rows, verified)
        assert got["exists"] == bool(all_matchings)
        assert got["unique"] == (len(all_matchings) == 1)
        if all_matchings:
            assert tuple(got["matching"]) in all_matchings
            for i in range(n):
                expect = sorted({mt[i] for mt in all_matchings})
                assert got["candidates"][i] == expect
            if len(all_matchings) == 1:
                only = next(iter(all_matchings))
                assert got["pairs"] == list(zip([b for b, _ in rows], only))
