"""订单变更包：版本校验、包内/基准冲突、原子切换、崩溃回滚、幂等重试。"""
from __future__ import annotations

from app import changes, pipeline, store

HEADER = "op,order_id,site,category,order_date,amount,qty,memo\n"


def make_package(package_id: str, base_version: int, ops: list[str], *,
                 header: str = HEADER) -> bytes:
    lines = [
        "# csvgate-change-package v1",
        f"# package_id: {package_id}",
        f"# base_version: {base_version}",
        header.rstrip("\n"),
    ]
    lines.extend(ops)
    return ("\n".join(lines) + "\n").encode("utf-8")


def package(package_id: str, base_version: int, *ops: str) -> bytes:
    return make_package(package_id, base_version, list(ops))


ADD = "add,710001,beijing,electronics,2026-11-15,299.00,2,新客首单"
REPRICE = "reprice,700003,guangzhou,apparel,2026-10-26,19.74,8,"
CANCEL = "cancel,700004,,,,,,"


def fresh_conn(tmp_path, name: str = "changes.sqlite3"):
    conn = store.connect(tmp_path / name)
    store.init_schema(conn)
    info = changes.ensure_baseline_version(conn)
    assert info["created"] is True or store.get_current_version(conn) == 1
    return conn


def test_baseline_version_is_complete_snapshot(tmp_path):
    conn = fresh_conn(tmp_path)
    assert store.get_current_version(conn) == 1
    assert store.count_version_orders(conn, 1) == 60
    check = changes.verify_version(conn, 1)
    assert check["ok"] is True, check
    # 幂等：再次调用不重复建版本
    again = changes.ensure_baseline_version(conn)
    assert again["created"] is False
    assert store.max_version_no(conn) == 1


def test_apply_package_switches_snapshot_and_site_summary(tmp_path):
    conn = fresh_conn(tmp_path)
    before = store.get_version(conn, 1)
    result = pipeline.run_change_package(package("PKG-A", 1, ADD, REPRICE, CANCEL),
                                        "a.csv", conn=conn)

    assert result["status"] == "committed", result
    assert result["version_no"] == 2
    assert result["previous_version"] == 1
    assert result["op_counts"] == {"add": 1, "reprice": 1, "cancel": 1}
    assert store.get_current_version(conn) == 2

    after = store.get_version(conn, 2)
    assert after["row_count"] == 60                      # +1 新增 -1 取消
    assert changes.verify_version(conn, 2)["ok"] is True
    assert sum(g["row_count"] for g in after["summary"]) == 60

    # 改价生效、取消的下单不再出现在当前快照
    orders = {r["order_id"]: r for r in conn.execute(
        "SELECT * FROM version_orders WHERE version_no = 2").fetchall()}
    assert orders[700003]["amount_cents"] == 1974
    assert 700004 not in orders
    assert orders[710001]["amount_cents"] == 29900

    # 站点汇总随之变化：+beijing 1 单、-shenzhen 1 单
    old_sites = {g["site"]: g["row_count"] for g in before["summary"]}
    new_sites = {g["site"]: g["row_count"] for g in after["summary"]}
    assert new_sites["beijing"] == old_sites["beijing"] + 1
    assert new_sites["shenzhen"] == old_sites["shenzhen"] - 1

    # 旧版本完整保留，可供复核
    assert store.get_version(conn, 1)["row_count"] == 60
    assert changes.verify_version(conn, 1)["ok"] is True


def test_retry_same_package_is_idempotent(tmp_path):
    conn = fresh_conn(tmp_path)
    raw = package("PKG-R", 1, ADD)
    first = pipeline.run_change_package(raw, "r.csv", conn=conn)
    assert first["status"] == "committed"

    for _ in range(3):
        again = pipeline.run_change_package(raw, "r.csv", conn=conn)
        assert again["status"] == "already_applied"
        assert again["version_no"] == first["version_no"]
        assert again["written"] == 0

    # 同一 package_id、不同文件内容也不重复应用
    other = pipeline.run_change_package(package("PKG-R", 2, REPRICE), "r2.csv", conn=conn)
    assert other["status"] in ("already_applied", "conflict")
    assert store.get_current_version(conn) == first["version_no"]
    assert store.max_version_no(conn) == first["version_no"]
    # 只有一次 committed 记录 + 若干 already_applied，没有多出来的版本
    statuses = [a["status"] for a in store.list_change_attempts(conn)]
    assert statuses.count("committed") == 1


def test_stale_base_is_rejected_and_lists_conflicting_orders(tmp_path):
    conn = fresh_conn(tmp_path)
    first = pipeline.run_change_package(package("PKG-1", 1, ADD, REPRICE), "1.csv", conn=conn)
    assert first["status"] == "committed"

    # 第二个班次仍基于版本 1 提交，且改到同一订单 → 整包拒绝
    stale_raw = package("PKG-2", 1, "reprice,700003,guangzhou,apparel,2026-10-26,88.00,8,",
                        "add,710009,chengdu,grocery,2026-11-15,5.00,1,")
    stale = pipeline.run_change_package(stale_raw, "2.csv", conn=conn)
    assert stale["status"] == "stale_base"
    assert stale["conflict_order_ids"] == [700003]
    assert store.get_current_version(conn) == 2
    assert store.max_version_no(conn) == 2               # 没有写入新版本
    assert store.count_version_orders(conn, 2) == 61

    attempt = store.get_change_attempt(conn, stale["attempt_id"])
    assert attempt["conflicts"][0]["code"] == "changed_after_base"
    assert attempt["conflicts"][0]["line_no"] == 5


def test_future_base_is_rejected(tmp_path):
    conn = fresh_conn(tmp_path)
    result = pipeline.run_change_package(package("PKG-F", 99, ADD), "f.csv", conn=conn)
    assert result["status"] == "future_base"
    assert store.get_current_version(conn) == 1


def test_in_package_duplicate_order_rejects_whole_package(tmp_path):
    conn = fresh_conn(tmp_path)
    result = pipeline.run_change_package(
        package("PKG-D", 1, "reprice,700003,guangzhou,apparel,2026-10-26,19.74,8,",
                "cancel,700003,,,,,,", ADD),
        "d.csv", conn=conn,
    )
    assert result["status"] == "conflict"
    assert result["conflict_order_ids"] == [700003]
    assert result["conflicts"][0]["code"] == "duplicate_in_package"
    assert result["conflicts"][0]["line_nos"] == [5, 6]
    assert store.get_current_version(conn) == 1
    assert store.max_version_no(conn) == 1


def test_base_conflicts_add_existing_and_missing_target(tmp_path):
    conn = fresh_conn(tmp_path)
    raw = package("PKG-C", 1,
                  "add,700001,beijing,electronics,2026-10-26,12.00,1,",
                  "reprice,799999,beijing,grocery,2026-11-15,10.00,1,")
    result = pipeline.run_change_package(raw, "c.csv", conn=conn)
    assert result["status"] == "conflict"
    assert sorted(result["conflict_order_ids"]) == [700001, 799999]
    codes = {c["code"] for c in result["conflicts"]}
    assert codes == {"order_exists", "order_not_found"}
    assert store.get_current_version(conn) == 1


def test_field_mismatch_against_base_is_conflict(tmp_path):
    conn = fresh_conn(tmp_path)
    raw = package("PKG-M", 1,
                  "reprice,700003,shanghai,apparel,2026-10-26,19.74,8,")
    result = pipeline.run_change_package(raw, "m.csv", conn=conn)
    assert result["status"] == "conflict"
    conflict = result["conflicts"][0]
    assert conflict["code"] == "field_mismatch"
    assert conflict["field"] == "site"
    assert conflict["expected"] == "guangzhou"
    assert store.get_current_version(conn) == 1


def test_crash_mid_switch_rolls_back_to_last_complete_version(tmp_path):
    conn = fresh_conn(tmp_path)
    raw = package("PKG-X", 1, ADD, REPRICE, CANCEL)
    crashed = pipeline.run_change_package(raw, "x.csv", conn=conn, fail_after=40)

    assert crashed["status"] == "rolled_back"
    assert "注入故障" in crashed["detail"]
    assert store.get_current_version(conn) == 1
    assert store.max_version_no(conn) == 1
    assert conn.execute("SELECT COUNT(*) AS c FROM version_orders WHERE version_no = 2"
                        ).fetchone()["c"] == 0
    assert conn.execute("SELECT COUNT(*) AS c FROM version_site_summary WHERE version_no = 2"
                        ).fetchone()["c"] == 0
    assert changes.verify_version(conn, 1)["ok"] is True

    # 重试同一包可以成功，且只计入一次
    retried = pipeline.run_change_package(raw, "x.csv", conn=conn)
    assert retried["status"] == "committed"
    assert store.get_current_version(conn) == 2
    repeat = pipeline.run_change_package(raw, "x.csv", conn=conn)
    assert repeat["status"] == "already_applied"
    assert store.max_version_no(conn) == 2


def test_recover_current_moves_dangling_pointer_back(tmp_path):
    conn = fresh_conn(tmp_path)
    pipeline.run_change_package(package("PKG-1", 1, ADD), "1.csv", conn=conn)
    pipeline.run_change_package(package("PKG-2", 2, REPRICE), "2.csv", conn=conn)
    assert store.get_current_version(conn) == 3

    # 模拟指针指向一个不存在的版本 / 缺失
    store.set_current_version(conn, 99)
    recovered = changes.recover_current(conn)
    assert recovered["recovered"] is True
    assert recovered["to"] == 3 == store.get_current_version(conn)

    conn.execute("DELETE FROM meta WHERE key = 'current_order_version'")
    recovered = changes.recover_current(conn)
    assert recovered["recovered"] is True
    assert store.get_current_version(conn) == 3


def test_tampered_incomplete_version_is_not_current(tmp_path):
    conn = fresh_conn(tmp_path)
    pipeline.run_change_package(package("PKG-1", 1, ADD), "1.csv", conn=conn)
    # 模拟掉电后残留的不完整版本（有版本头、没有明细和汇总）
    conn.execute(
        "INSERT INTO order_versions(version_no, package_id, base_version, filename, sha256,"
        " op_count, add_count, reprice_count, cancel_count, row_count, total_amount_cents,"
        " total_qty, status, created_at) VALUES(3,'PKG-BAD',2,'bad.csv',?,1,1,0,0,60,0,0,"
        "'committed',?)",
        ("b" * 64, store.now_iso()),
    )
    store.set_current_version(conn, 3)
    assert changes.verify_version(conn, 3)["ok"] is False

    assert changes.last_complete_version(conn) == 2
    recovered = changes.recover_current(conn)
    assert recovered["recovered"] is True
    assert recovered["to"] == 2
    assert store.get_current_version(conn) == 2


def test_package_format_errors_reject_without_writing(tmp_path):
    conn = fresh_conn(tmp_path)
    cases = {
        "no_base": make_package("PKG-E1", 0, [ADD]).replace(b"# base_version: 0\n", b""),
        "bad_op": package("PKG-E2", 1, "update,700003,,,,,,"),
        "cancel_with_fields": package("PKG-E3", 1, "cancel,700004,beijing,grocery,2026-11-15,1.00,1,"),
        "add_missing_fields": package("PKG-E4", 1, "add,710005,beijing,,,,,"),
        "bad_money": package("PKG-E5", 1, 'add,710006,beijing,grocery,2026-11-15,"1,000.00",1,'),
        "bad_date": package("PKG-E6", 1, "add,710007,beijing,grocery,2026-02-30,10.00,1,"),
        "empty": make_package("PKG-E7", 1, []),
    }
    expected_codes = {
        "no_base": "base_version_missing",
        "bad_op": "op_invalid",
        "cancel_with_fields": "unexpected_field",
        "add_missing_fields": "required",
        "bad_money": "decimal_type",
        "bad_date": "date_invalid",
        "empty": "empty_package",
    }
    for name, raw in cases.items():
        result = pipeline.run_change_package(raw, f"{name}.csv", conn=conn)
        assert result["status"] == "rejected", (name, result)
        assert expected_codes[name] in {e["code"] for e in result["errors"]}, (name, result["errors"])
    assert store.get_current_version(conn) == 1
    assert store.max_version_no(conn) == 1


def test_header_contract_for_package(tmp_path):
    conn = fresh_conn(tmp_path)
    bad_header = "op,order_id,site,category,order_date,amount,qty,note\n"
    result = pipeline.run_change_package(
        make_package("PKG-H", 1, ["add,710001,beijing,electronics,2026-11-15,299.00,2,x"],
                     header=bad_header),
        "h.csv", conn=conn,
    )
    assert result["status"] == "rejected"
    assert "extra_column" in {e["code"] for e in result["errors"]}

    dup_header = "op,order_id,order_id,site,category,order_date,amount,qty,memo\n"
    result = pipeline.run_change_package(
        make_package("PKG-H2", 1, ["add,710001,700001,beijing,electronics,2026-11-15,299.00,2,x"],
                     header=dup_header),
        "h2.csv", conn=conn,
    )
    assert result["status"] == "rejected"
    assert "duplicate_column" in {e["code"] for e in result["errors"]}


def test_encoding_shared_with_import_pipeline(tmp_path):
    conn = fresh_conn(tmp_path)
    raw = package("PKG-GBK", 1, "add,710010,beijing,grocery,2026-11-15,10.00,1,华东仓")
    raw = raw.decode("utf-8").encode("gb18030")
    result = pipeline.run_change_package(raw, "gbk.csv", conn=conn)
    assert result["status"] == "committed", result
    assert result["encoding"]["encoding"] == "gb18030"

    bad = pipeline.run_change_package(b"\x80\x81\xff\xfe", "bad.csv", conn=conn)
    assert bad["status"] == "encoding_failed"
    assert store.get_current_version(conn) == 2


def test_sequential_packages_build_on_each_other(tmp_path):
    conn = fresh_conn(tmp_path)
    one = pipeline.run_change_package(package("PKG-S1", 1, ADD), "1.csv", conn=conn)
    assert one["status"] == "committed"
    two = pipeline.run_change_package(
        package("PKG-S2", 2, "reprice,710001,beijing,electronics,2026-11-15,199.00,2,"),
        "2.csv", conn=conn)
    assert two["status"] == "committed"
    assert store.get_current_version(conn) == 3
    row = conn.execute("SELECT amount_cents FROM version_orders WHERE version_no = 3"
                       " AND order_id = 710001").fetchone()
    assert row["amount_cents"] == 19900
    assert store.get_version(conn, 3)["row_count"] == 61
