"""HTTP 端到端：变更包上传、基准过期、包内冲突、幂等重试、页面与版本 API。"""
from __future__ import annotations

from tests.test_changes import ADD, package


def upload(client, raw, name="change.csv"):
    return client.post("/api/change-packages",
                       files={"file": (name, raw, "text/csv")})


def test_health_reports_current_version_and_integrity(client):
    body = client.get("/health").json()
    assert body["current_order_version"] == 1
    assert body["order_rows"] == 60
    assert body["current_version_complete"] is True
    assert body["fact_rows"] == 60


def test_sample_packages_are_served(client):
    for name in ("change_ok.csv", "change_conflict.csv", "change_stale.csv"):
        res = client.get(f"/api/samples/{name}")
        assert res.status_code == 200, name
        assert res.content.startswith(b"# csvgate-change-package")


def test_apply_sample_package_then_retry(client):
    raw = client.get("/api/samples/change_ok.csv").content
    first = upload(client, raw, "change_ok.csv")
    assert first.status_code == 200
    body = first.json()
    assert body["status"] == "committed"
    assert body["version_no"] == 2
    assert body["previous_version"] == 1
    assert body["op_counts"] == {"add": 1, "reprice": 1, "cancel": 1}
    assert body["conflict_order_ids"] == []

    health = client.get("/health").json()
    assert health["current_order_version"] == 2
    assert health["order_rows"] == 60
    assert health["current_version_complete"] is True

    retry = upload(client, raw, "change_ok.csv")
    assert retry.status_code == 200
    assert retry.json()["status"] == "already_applied"
    assert retry.json()["version_no"] == 2
    assert retry.json()["written"] == 0
    assert client.get("/health").json()["current_order_version"] == 2

    versions = client.get("/api/change-versions").json()
    assert versions["current_version"] == 2
    assert [v["version_no"] for v in versions["versions"]] == [2, 1]


def test_conflict_package_returns_409_with_order_ids(client):
    raw = client.get("/api/samples/change_conflict.csv").content
    res = upload(client, raw, "change_conflict.csv")
    assert res.status_code == 409
    body = res.json()
    assert body["status"] == "conflict"
    assert body["conflict_order_ids"] == [700003]
    assert body["conflicts"][0]["code"] == "duplicate_in_package"
    assert client.get("/health").json()["current_order_version"] == 1


def test_stale_base_returns_409_and_keeps_current(client):
    ok = upload(client, client.get("/api/samples/change_ok.csv").content).json()
    assert ok["status"] == "committed"
    stale = upload(client, client.get("/api/samples/change_stale.csv").content,
                   "change_stale.csv")
    assert stale.status_code == 409
    body = stale.json()
    assert body["status"] == "stale_base"
    assert body["base_version"] == 1
    assert body["current_version"] == 2
    assert client.get("/health").json()["current_order_version"] == 2


def test_two_shift_overwrite_is_prevented(client):
    """两个班次都基于版本 1 提交：先到的成功，后到的整包拒绝，不会互相覆盖。"""
    night = package("PKG-NIGHT", 1, ADD,
                    "reprice,700003,guangzhou,apparel,2026-10-26,15.00,8,")
    day = package("PKG-DAY", 1, "reprice,700003,guangzhou,apparel,2026-10-26,50.00,8,")

    first = upload(client, night, "night.csv")
    assert first.json()["status"] == "committed"
    second = upload(client, day, "day.csv")
    assert second.status_code == 409
    assert second.json()["status"] == "stale_base"
    assert second.json()["conflict_order_ids"] == [700003]

    # 当前订单只包含先到包的结果：700003 没有被后到的包改价成功
    version = client.get("/api/change-versions/2").json()["version"]
    orders = {r["order_id"]: r for r in version["sample_orders"]}
    changed = {c["order_id"]: c["op"] for c in version["changes"]}
    assert changed == {710001: "add", 700003: "reprice"}
    assert orders[700003]["amount_cents"] == 1500          # 先到包的新价，未被后到包覆盖
    assert client.get("/health").json()["current_order_version"] == 2


def test_pages_render_after_change(client):
    assert client.get("/changes").status_code == 200
    upload(client, client.get("/api/samples/change_ok.csv").content)
    for path in ("/", "/changes", "/changes/version/1", "/changes/version/2"):
        res = client.get(path)
        assert res.status_code == 200, path
        assert "当前订单" in res.text or "订单版本" in res.text, path

    uploaded = upload(client, package("PKG-P", 2, ADD)).json()
    detail = client.get(f"/changes/attempt/{uploaded['attempt_id']}")
    assert detail.status_code == 200
    assert "文件证据" in detail.text


def test_change_attempt_page_lists_conflicts(client):
    upload(client, client.get("/api/samples/change_conflict.csv").content)
    res = upload(client, package("PKG-CD", 1, "cancel,799999,,,,,,", ADD))
    attempt_id = res.json()["attempt_id"]
    page = client.get(f"/changes/attempt/{attempt_id}")
    assert page.status_code == 200
    assert "冲突订单" in page.text
    assert "799999" in page.text


def test_workbench_form_rejection_renders_conflict_ids(client):
    res = client.post("/changes", files={
        "file": ("change_conflict.csv",
                 client.get("/api/samples/change_conflict.csv").content, "text/csv")})
    assert res.status_code == 409
    assert "整包拒绝" in res.text
    assert "700003" in res.text


def test_unknown_version_or_attempt_404(client):
    assert client.get("/changes/version/999").status_code == 404
    assert client.get("/changes/attempt/999").status_code == 404
    assert client.get("/api/change-versions/999").status_code == 404
