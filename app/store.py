"""SQLite 访问（薄封装，无 ORM）。

表：
- meta(key,value)                  seed 版本
- batches(...)                     已入库批次，sha256 用于重复文件判定
- fact_orders(...)                 明细事实，仅 committed 批次写入
- batch_aggregates(...)            按 site 聚合快照
- import_attempts(...)             每次上传的结论（含回滚记录）/review 用
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from . import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  encoding TEXT NOT NULL,
  bom INTEGER NOT NULL DEFAULT 0,
  row_count INTEGER NOT NULL,
  total_amount_cents INTEGER NOT NULL,
  total_qty INTEGER NOT NULL,
  forced INTEGER NOT NULL DEFAULT 0,
  superseded_by TEXT,
  created_at TEXT NOT NULL,
  note TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_sha ON batches(sha256);

CREATE TABLE IF NOT EXISTS fact_orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL,
  row_no INTEGER NOT NULL,
  order_id INTEGER NOT NULL,
  site TEXT NOT NULL,
  category TEXT NOT NULL,
  order_date TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  memo TEXT
);
CREATE INDEX IF NOT EXISTS idx_fact_batch ON fact_orders(batch_id);

CREATE TABLE IF NOT EXISTS batch_aggregates (
  batch_id TEXT NOT NULL,
  site TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  row_count INTEGER NOT NULL,
  PRIMARY KEY (batch_id, site)
);

CREATE TABLE IF NOT EXISTS import_attempts (
  attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  encoding TEXT,
  status TEXT NOT NULL,
  error_count INTEGER NOT NULL DEFAULT 0,
  row_count INTEGER NOT NULL DEFAULT 0,
  batch_id TEXT,
  detail TEXT,
  errors_json TEXT,
  created_at TEXT NOT NULL
);

-- ---------------------------------------------------------------- 订单变更包
-- 版本化快照：每个版本是一份完整、自洽的当前订单，切换只改 meta.current_order_version。
-- 中断时版本行与快照行同事务回滚，指针仍停在最后完整版本。
CREATE TABLE IF NOT EXISTS order_versions (
  version_no INTEGER PRIMARY KEY,
  package_id TEXT NOT NULL,
  base_version INTEGER NOT NULL,
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  op_count INTEGER NOT NULL DEFAULT 0,
  add_count INTEGER NOT NULL DEFAULT 0,
  reprice_count INTEGER NOT NULL DEFAULT 0,
  cancel_count INTEGER NOT NULL DEFAULT 0,
  row_count INTEGER NOT NULL,
  total_amount_cents INTEGER NOT NULL,
  total_qty INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'committed',
  created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_versions_package ON order_versions(package_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_versions_sha ON order_versions(sha256);

CREATE TABLE IF NOT EXISTS version_orders (
  version_no INTEGER NOT NULL,
  order_id INTEGER NOT NULL,
  site TEXT NOT NULL,
  category TEXT NOT NULL,
  order_date TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  memo TEXT,
  PRIMARY KEY (version_no, order_id)
);
CREATE INDEX IF NOT EXISTS idx_version_orders_v ON version_orders(version_no);

CREATE TABLE IF NOT EXISTS version_site_summary (
  version_no INTEGER NOT NULL,
  site TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  row_count INTEGER NOT NULL,
  PRIMARY KEY (version_no, site)
);

CREATE TABLE IF NOT EXISTS version_changes (
  version_no INTEGER NOT NULL,
  order_id INTEGER NOT NULL,
  op TEXT NOT NULL,
  PRIMARY KEY (version_no, order_id)
);

CREATE TABLE IF NOT EXISTS change_attempts (
  attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
  package_id TEXT NOT NULL DEFAULT '',
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  base_version INTEGER,
  current_version INTEGER,
  status TEXT NOT NULL,
  op_count INTEGER NOT NULL DEFAULT 0,
  conflict_count INTEGER NOT NULL DEFAULT 0,
  version_no INTEGER,
  detail TEXT,
  conflicts_json TEXT,
  errors_json TEXT,
  encoding TEXT,
  created_at TEXT NOT NULL
);
"""

# 允许的上传策略（避免路由随意传值）
STATUSES = ("committed", "duplicate_file", "rejected", "encoding_failed", "rolled_back")


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def connect(path: Path | str | None = None) -> sqlite3.Connection:
    target = Path(path) if path else settings.DB_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target), isolation_level=None, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def count_rows(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) AS c FROM fact_orders").fetchone()["c"]


def find_batch_by_sha(conn: sqlite3.Connection, sha256: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM batches WHERE sha256 = ? ORDER BY created_at ASC LIMIT 1", (sha256,)
    ).fetchone()


def list_batches(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM batches ORDER BY created_at DESC, batch_id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [
        {
            "batch_id": r["batch_id"],
            "filename": r["filename"],
            "sha256": r["sha256"],
            "encoding": r["encoding"],
            "bom": bool(r["bom"]),
            "row_count": r["row_count"],
            "total_amount": f"{r['total_amount_cents'] // 100}.{r['total_amount_cents'] % 100:02d}",
            "total_qty": r["total_qty"],
            "forced": bool(r["forced"]),
            "superseded_by": r["superseded_by"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]


def get_batch(conn: sqlite3.Connection, batch_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
    if not row:
        return None
    agg = [
        {
            "site": a["site"],
            "amount": f"{a['amount_cents'] // 100}.{a['amount_cents'] % 100:02d}",
            "amount_cents": a["amount_cents"],
            "qty": a["qty"],
            "row_count": a["row_count"],
        }
        for a in conn.execute(
            "SELECT * FROM batch_aggregates WHERE batch_id = ? ORDER BY amount_cents DESC",
            (batch_id,),
        ).fetchall()
    ]
    sample = [
        dict(r) for r in conn.execute(
            "SELECT row_no, order_id, site, category, order_date, amount_cents, qty, memo "
            "FROM fact_orders WHERE batch_id = ? ORDER BY row_no LIMIT 20",
            (batch_id,),
        ).fetchall()
    ]
    for r in sample:
        r["amount"] = f"{r['amount_cents'] // 100}.{r['amount_cents'] % 100:02d}"
    return {
        "batch_id": row["batch_id"],
        "filename": row["filename"],
        "sha256": row["sha256"],
        "encoding": row["encoding"],
        "bom": bool(row["bom"]),
        "row_count": row["row_count"],
        "total_amount": f"{row['total_amount_cents'] // 100}.{row['total_amount_cents'] % 100:02d}",
        "total_amount_cents": row["total_amount_cents"],
        "total_qty": row["total_qty"],
        "forced": bool(row["forced"]),
        "superseded_by": row["superseded_by"],
        "created_at": row["created_at"],
        "note": row["note"],
        "aggregates": agg,
        "sample_rows": sample,
    }


def list_attempts(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM import_attempts ORDER BY attempt_id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_attempt(conn: sqlite3.Connection, attempt_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM import_attempts WHERE attempt_id = ?", (attempt_id,)
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    try:
        data["errors"] = json.loads(data.get("errors_json") or "[]")
    except json.JSONDecodeError:
        data["errors"] = []
    return data


def record_attempt(conn: sqlite3.Connection, *, filename: str, sha256: str, status: str,
                   encoding: str = "", error_count: int = 0, row_count: int = 0,
                   batch_id: str | None = None, detail: str = "",
                   errors: list[dict] | None = None) -> int:
    payload = json.dumps(errors[:200] if errors else [], ensure_ascii=False)
    cur = conn.execute(
        "INSERT INTO import_attempts(filename, sha256, encoding, status, error_count,"
        " row_count, batch_id, detail, errors_json, created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?)",
        (filename, sha256, encoding, status, error_count, row_count, batch_id, detail,
         payload, now_iso()),
    )
    return int(cur.lastrowid or 0)


def stats(conn: sqlite3.Connection) -> dict:
    batches = conn.execute("SELECT COUNT(*) AS c FROM batches").fetchone()["c"]
    fact = count_rows(conn)
    rejected = conn.execute(
        "SELECT COUNT(*) AS c FROM import_attempts WHERE status IN ('rejected','encoding_failed')"
    ).fetchone()["c"]
    sites = conn.execute("SELECT COUNT(DISTINCT site) AS c FROM fact_orders").fetchone()["c"]
    return {"batches": batches, "fact_rows": fact, "rejected_attempts": rejected, "sites": sites}


# ---------------------------------------------------------------- 订单变更包 / 版本快照

CURRENT_ORDER_META_KEY = "current_order_version"


def get_current_version(conn: sqlite3.Connection) -> int | None:
    """当前订单版本号；只由 commit 事务推进，是「最后完整版本」的唯一指针。"""
    value = get_meta(conn, CURRENT_ORDER_META_KEY)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def set_current_version(conn: sqlite3.Connection, version_no: int) -> None:
    set_meta(conn, CURRENT_ORDER_META_KEY, str(version_no))


def max_version_no(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(version_no) AS v FROM order_versions").fetchone()
    return int(row["v"] or 0)


def next_version_no(conn: sqlite3.Connection) -> int:
    return max_version_no(conn) + 1


def find_version_by_sha(conn: sqlite3.Connection, sha256: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM order_versions WHERE sha256 = ? LIMIT 1", (sha256,)
    ).fetchone()


def find_version_by_package(conn: sqlite3.Connection, package_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM order_versions WHERE package_id = ? LIMIT 1", (package_id,)
    ).fetchone()


def count_version_orders(conn: sqlite3.Connection, version_no: int) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS c FROM version_orders WHERE version_no = ?", (version_no,)
    ).fetchone()["c"]


def get_version(conn: sqlite3.Connection, version_no: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM order_versions WHERE version_no = ?", (version_no,)
    ).fetchone()
    if not row:
        return None
    summary = [
        {
            "site": r["site"],
            "amount": f"{r['amount_cents'] // 100}.{r['amount_cents'] % 100:02d}",
            "amount_cents": r["amount_cents"],
            "qty": r["qty"],
            "row_count": r["row_count"],
        }
        for r in conn.execute(
            "SELECT * FROM version_site_summary WHERE version_no = ? "
            "ORDER BY amount_cents DESC, site",
            (version_no,),
        ).fetchall()
    ]
    changes = [
        dict(r) for r in conn.execute(
            "SELECT order_id, op FROM version_changes WHERE version_no = ? ORDER BY order_id",
            (version_no,),
        ).fetchall()
    ]
    orders = [
        dict(r) for r in conn.execute(
            "SELECT order_id, site, category, order_date, amount_cents, qty, memo "
            "FROM version_orders WHERE version_no = ? ORDER BY order_id LIMIT 20",
            (version_no,),
        ).fetchall()
    ]
    for o in orders:
        o["amount"] = f"{o['amount_cents'] // 100}.{o['amount_cents'] % 100:02d}"
    return {
        "version_no": row["version_no"],
        "package_id": row["package_id"],
        "base_version": row["base_version"],
        "filename": row["filename"],
        "sha256": row["sha256"],
        "op_count": row["op_count"],
        "add_count": row["add_count"],
        "reprice_count": row["reprice_count"],
        "cancel_count": row["cancel_count"],
        "row_count": row["row_count"],
        "total_amount": f"{row['total_amount_cents'] // 100}.{row['total_amount_cents'] % 100:02d}",
        "total_amount_cents": row["total_amount_cents"],
        "total_qty": row["total_qty"],
        "status": row["status"],
        "created_at": row["created_at"],
        "summary": summary,
        "changes": changes,
        "sample_orders": orders,
    }


def list_versions(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM order_versions ORDER BY version_no DESC LIMIT ?", (limit,)
    ).fetchall()
    current = get_current_version(conn)
    return [
        {
            "version_no": r["version_no"],
            "package_id": r["package_id"],
            "base_version": r["base_version"],
            "filename": r["filename"],
            "sha256": r["sha256"],
            "op_count": r["op_count"],
            "add_count": r["add_count"],
            "reprice_count": r["reprice_count"],
            "cancel_count": r["cancel_count"],
            "row_count": r["row_count"],
            "total_amount": f"{r['total_amount_cents'] // 100}.{r['total_amount_cents'] % 100:02d}",
            "total_qty": r["total_qty"],
            "created_at": r["created_at"],
            "is_current": r["version_no"] == current,
        }
        for r in rows
    ]


def record_change_attempt(
    conn: sqlite3.Connection,
    *,
    filename: str,
    sha256: str,
    status: str,
    package_id: str = "",
    base_version: int | None = None,
    current_version: int | None = None,
    op_count: int = 0,
    conflict_count: int = 0,
    version_no: int | None = None,
    detail: str = "",
    conflicts: list[dict] | None = None,
    errors: list[dict] | None = None,
    encoding: str = "",
) -> int:
    payload = json.dumps((conflicts or [])[:200], ensure_ascii=False)
    payload_errors = json.dumps((errors or [])[:200], ensure_ascii=False)
    cur = conn.execute(
        "INSERT INTO change_attempts(package_id, filename, sha256, base_version,"
        " current_version, status, op_count, conflict_count, version_no, detail,"
        " conflicts_json, errors_json, encoding, created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (package_id, filename, sha256, base_version, current_version, status,
         op_count, conflict_count, version_no, detail, payload, payload_errors,
         encoding, now_iso()),
    )
    return int(cur.lastrowid or 0)


def list_change_attempts(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM change_attempts ORDER BY attempt_id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_change_attempt(conn: sqlite3.Connection, attempt_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM change_attempts WHERE attempt_id = ?", (attempt_id,)
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    for key, target in (("conflicts_json", "conflicts"), ("errors_json", "errors")):
        try:
            data[target] = json.loads(data.get(key) or "[]")
        except json.JSONDecodeError:
            data[target] = []
    return data


def change_stats(conn: sqlite3.Connection) -> dict:
    current = get_current_version(conn)
    versions = conn.execute("SELECT COUNT(*) AS c FROM order_versions").fetchone()["c"]
    rejected = conn.execute(
        "SELECT COUNT(*) AS c FROM change_attempts"
        " WHERE status IN ('rejected','encoding_failed','rolled_back','conflict',"
        "                  'stale_base','future_base')"
    ).fetchone()["c"]
    return {
        "current_order_version": current,
        "order_versions": versions,
        "change_rejected": rejected,
    }
