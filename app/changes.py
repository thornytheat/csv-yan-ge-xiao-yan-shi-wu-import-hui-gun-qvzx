"""订单变更包：版本化快照、整包校验、原子切换与幂等重试。

包格式（CSV，前置若干 `#` 元数据行）：

    # csvgate-change-package v1
    # package_id: PKG-20261115-001
    # base_version: 1
    op,order_id,site,category,order_date,amount,qty,memo
    add,710001,beijing,electronics,2026-11-15,299.00,2,新客首单
    reprice,700003,guangzhou,apparel,2026-10-26,19.74,8,
    cancel,700004,,,,,,

语义：
- 每个包基于一个明确的 base_version（当前订单版本），包内每个 order_id 最多出现一次
  （新增 / 改价 / 取消三选一）。
- 版本 = 完整、自洽的「当前订单 + 站点汇总」快照。应用变更包不会增量改旧版本，而是
  基于基准版本算出新快照，并在**同一个事务**里写新版本 + 新汇总 + 推进 meta 指针；
  事务失败或进程中断 → 整体回滚，指针仍停在最后完整版本。
- 基准版本不是当前版本 → 整包拒绝（stale_base / future_base），避免两个班次互相覆盖。
- 包内订单重复、add 已存在、reprice/cancel 不存在或字段与基准不符 → 整包拒绝并列出冲突订单。
- 同一 sha256 或同一 package_id 的包已成功应用 → already_applied，不重复计入。
"""
from __future__ import annotations

import csv
import hashlib
import re
import sqlite3
from datetime import date
from decimal import Decimal, InvalidOperation

from . import aggregate, contract, encoding, store

CHANGE_COLUMNS = ["op", "order_id", "site", "category", "order_date", "amount", "qty", "memo"]
HEADER_REQUIRED = ["op", "order_id"]
OPS = ("add", "reprice", "cancel")
META_KEYS = {"package_id", "base_version", "note"}

INT_RE = re.compile(r"^[+-]?\d+$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DEC_RE = re.compile(r"^\d{1,10}(\.\d{1,6})?$")

ROW_LABELS = {
    "required": "必填为空",
    "length": "超出长度上限",
    "int_type": "整数字段格式错误",
    "int_range": "整数超出范围",
    "date_format": "日期格式错误",
    "date_invalid": "日期不存在",
    "decimal_type": "金额格式错误",
    "decimal_scale": "金额小数位超限",
    "decimal_range": "金额超出允许范围",
    "enum": "枚举值越界",
    "op_invalid": "变更类型非法",
    "unexpected_field": "该变更类型不允许填此字段",
    "column_count": "列数与表头不一致",
    "missing_column": "表头缺列",
    "extra_column": "表头出现契约外列",
    "duplicate_column": "表头重复列",
    "empty_header": "表头为空",
    "blank_column_name": "表头列名为空",
    "package_meta_invalid": "包元数据格式错误",
    "package_meta_unknown": "包元数据未知键",
    "base_version_invalid": "基准版本非法",
    "base_version_missing": "缺少基准版本",
    "comment_after_header": "数据区出现注释行",
    "empty_package": "包内没有变更行",
}

CONFLICT_LABELS = {
    "duplicate_in_package": "包内订单重复",
    "order_exists": "新增的订单已存在",
    "order_not_found": "改价/取消的订单不存在",
    "field_mismatch": "字段与基准版本不一致",
    "package_id_conflict": "package_id 已被另一份内容占用",
    "changed_after_base": "基准版本之后该订单已被改动",
}

# 数据区字段的契约（复用主契约的枚举/长度/格式）
FIELD_SPECS = {name: contract.COLUMN_SPECS[name] for name in
               ("site", "category", "order_date", "amount", "qty", "memo")}


def _err(line_no: int, row_no: int, column: str, code: str, raw: str, reason: str,
         label: str | None = None) -> dict:
    return {
        "line_no": line_no,
        "row_no": row_no,
        "column": column,
        "code": code,
        "label": label or ROW_LABELS.get(code, code),
        "raw": raw,
        "reason": reason,
    }


def _conflict(order_id: int | None, op: str, code: str, reason: str,
              line_no: int | None = None, **extra) -> dict:
    payload = {
        "order_id": order_id,
        "op": op,
        "code": code,
        "label": CONFLICT_LABELS.get(code, code),
        "reason": reason,
        "line_no": line_no,
    }
    payload.update(extra)
    return payload


def _split_records(text: str) -> tuple[dict, list[str], int, list[tuple[int, list[str]]], list[dict]]:
    """拆元数据 / 表头 / 数据行；行号一律是文件真实行号。"""
    meta: dict[str, str] = {}
    header: list[str] = []
    header_line = 0
    records: list[tuple[int, list[str]]] = []
    errors: list[dict] = []

    for line_no, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not header:
            if not stripped:
                continue
            if stripped.startswith("#"):
                body = stripped[1:].strip()
                if body.lower().startswith("csvgate-change-package"):
                    meta["format"] = body
                elif ":" in body:
                    key, value = body.split(":", 1)
                    key = key.strip().lower()
                    if key in META_KEYS:
                        meta[key] = value.strip()
                    else:
                        errors.append(_err(line_no, 0, "", "package_meta_unknown", key,
                                           f"未知的包元数据键「{key}」，只支持 "
                                           f"{', '.join(sorted(META_KEYS))}。"))
                else:
                    errors.append(_err(line_no, 0, "", "package_meta_invalid", body,
                                       "包元数据行应为 `# key: value` 形式。"))
                continue
            header = next(csv.reader([line]))
            header_line = line_no
            continue

        if not stripped:
            continue
        if stripped.startswith("#"):
            errors.append(_err(line_no, 0, "", "comment_after_header", stripped,
                               "表头之后不能再出现注释行；请把 # 元数据放到文件开头。"))
            continue
        records.append((line_no, next(csv.reader([line]))))

    return meta, header, header_line, records, errors


def _check_header(header: list[str]) -> tuple[list[str], dict[str, int], list[dict]]:
    errors: list[dict] = []
    cells = [(c or "").replace("﻿", "").strip() for c in header]
    if not cells:
        errors.append(_err(0, 0, "", "empty_header", "", "文件没有表头行。"))
        return cells, {}, errors

    if any(not c for c in cells):
        blank = [str(i + 1) for i, c in enumerate(cells) if not c]
        errors.append(_err(0, 0, "", "blank_column_name", ",".join(blank),
                           f"表头第 {', '.join(blank)} 个列名为空。"))
    duplicates = sorted({c for c in cells if c and cells.count(c) > 1}, key=cells.index)
    if duplicates:
        errors.append(_err(0, 0, "", "duplicate_column", ",".join(duplicates),
                           f"表头存在重复列：{', '.join(duplicates)}。"))
    missing = [c for c in HEADER_REQUIRED if c not in cells]
    if missing:
        errors.append(_err(0, 0, "", "missing_column", ",".join(missing),
                           f"缺少必需列：{', '.join(missing)}。变更包至少需要 op 与 order_id。"))
    extra = [c for c in cells if c not in CHANGE_COLUMNS]
    if extra:
        errors.append(_err(0, 0, "", "extra_column", ",".join(extra),
                           f"出现契约外列：{', '.join(extra)}；契约列为 "
                           f"{', '.join(CHANGE_COLUMNS)}。"))
    usable = [c for c in cells if c in CHANGE_COLUMNS and c not in duplicates]
    index = {name: cells.index(name) for name in usable}
    return cells, index, errors


def _validate_field(per_cell: dict[str, dict], values: dict[str, str], name: str,
                    line_no: int, row_no: int) -> None:
    """对已提供的字段做「长度 → 类型/枚举」校验，同一单元格只报第一个错。"""
    raw = values.get(name, "")
    if not raw or name in per_cell:
        return
    spec = FIELD_SPECS[name]
    limit = spec.get("max_length")
    if limit and len(raw) > limit:
        per_cell[name] = _err(line_no, row_no, name, "length", raw,
                              f"长度 {len(raw)} 超过上限 {limit}。")
        return

    if name == "order_date":
        if not DATE_RE.match(raw):
            per_cell[name] = _err(line_no, row_no, name, "date_format", raw,
                                  f"日期需为 YYYY-MM-DD，「{raw}」不符合。")
            return
        try:
            date.fromisoformat(raw)
        except ValueError as exc:
            per_cell[name] = _err(line_no, row_no, name, "date_invalid", raw,
                                  f"「{raw}」不是真实存在的日期（{exc}）。")
        return

    if name == "amount":
        if not DEC_RE.match(raw):
            per_cell[name] = _err(line_no, row_no, name, "decimal_type", raw,
                                  "金额需为纯数字（如 128.50），不允许千分位或货币符号。")
            return
        try:
            amount = Decimal(raw)
        except InvalidOperation:
            per_cell[name] = _err(line_no, row_no, name, "decimal_type", raw,
                                  f"「{raw}」无法解析为金额。")
            return
        exponent = -amount.as_tuple().exponent  # type: ignore[arg-type]
        if exponent > 2:
            per_cell[name] = _err(line_no, row_no, name, "decimal_scale", raw,
                                  f"小数位 {exponent} 位，超过 2 位。")
        elif amount <= 0:
            per_cell[name] = _err(line_no, row_no, name, "decimal_range", raw,
                                  f"金额需大于 0，当前 {raw}。")
        elif amount > Decimal("99999999.99"):
            per_cell[name] = _err(line_no, row_no, name, "decimal_range", raw,
                                  f"金额 {raw} 超过上限 99999999.99。")
        return

    if name == "qty":
        if not INT_RE.match(raw):
            per_cell[name] = _err(line_no, row_no, name, "int_type", raw,
                                  f"「{raw}」不是整数。")
            return
        number = int(raw)
        if number < 1 or number > 100000:
            per_cell[name] = _err(line_no, row_no, name, "int_range", raw,
                                  f"件数需在 1~100000，当前 {number}。")
        return

    if name in ("site", "category"):
        if raw not in spec["enum"]:
            per_cell[name] = _err(line_no, row_no, name, "enum", raw,
                                  f"「{raw}」不在允许值内：{', '.join(spec['enum'])}。")


def parse_package(text: str) -> dict:
    """解析并做不依赖基准版本的校验；返回包结构 + errors + 包内冲突。"""
    meta, header, header_line, records, meta_errors = _split_records(text)
    cells, index, header_errors = _check_header(header)
    errors: list[dict] = list(meta_errors) + list(header_errors)

    base_version: int | None = None
    raw_base = meta.get("base_version", "")
    if not raw_base:
        errors.append(_err(0, 0, "", "base_version_missing", "",
                           "包元数据缺少 `# base_version: <当前订单版本>`；没有基准版本的包一律拒绝。"))
    elif not re.match(r"^\d+$", raw_base):
        errors.append(_err(0, 0, "", "base_version_invalid", raw_base,
                           f"base_version「{raw_base}」不是非负整数。"))
    else:
        base_version = int(raw_base)

    rows: list[dict] = []
    if not errors:
        for row_no, (line_no, record) in enumerate(records, start=1):
            if len(record) != len(cells):
                errors.append(_err(line_no, row_no, "", "column_count", str(len(record)),
                                   f"本行有 {len(record)} 列，表头有 {len(cells)} 列。"))
                continue

            values = {name: (record[idx] or "").strip() for name, idx in index.items()}
            per_cell: dict[str, dict] = {}

            def add(column: str, code: str, raw: str, reason: str) -> None:
                if column not in per_cell:
                    per_cell[column] = _err(line_no, row_no, column, code, raw, reason)

            op = values.get("op", "")
            oid: int | None = None
            oid_raw = values.get("order_id", "")

            if not op:
                add("op", "required", "", "变更类型 op 为空；应为 add / reprice / cancel。")
            elif op not in OPS:
                add("op", "op_invalid", op,
                    f"「{op}」不是合法变更类型；只支持 add / reprice / cancel。")

            if not oid_raw:
                add("order_id", "required", "", "order_id 为空。")
            elif not INT_RE.match(oid_raw):
                add("order_id", "int_type", oid_raw, f"「{oid_raw}」不是整数。")
            else:
                oid = int(oid_raw)
                if oid <= 0:
                    add("order_id", "int_range", oid_raw, f"order_id 需为正整数，当前 {oid}。")

            # 各操作类型的必填 / 禁填字段
            if op == "add":
                for name in ("site", "category", "order_date", "amount", "qty"):
                    if not values.get(name):
                        add(name, "required", "",
                            f"新增订单缺少 {name}；add 需要完整订单字段。")
            elif op == "reprice":
                if not values.get("amount"):
                    add("amount", "required", "",
                        "改价订单缺少 amount；reprice 必须给出新金额。")
            elif op == "cancel":
                for name in ("site", "category", "order_date", "amount", "qty", "memo"):
                    if values.get(name):
                        add(name, "unexpected_field", values[name],
                            f"cancel 只按 order_id 取消，不应携带 {name}。")

            for name in FIELD_SPECS:
                _validate_field(per_cell, values, name, line_no, row_no)

            errors.extend(per_cell.values())
            if per_cell:
                continue

            rows.append({
                "line_no": line_no,
                "row_no": row_no,
                "op": op,
                "order_id": int(oid),  # type: ignore[arg-type]
                "site": values.get("site") or None,
                "category": values.get("category") or None,
                "order_date": values.get("order_date") or None,
                "amount_cents": (int(Decimal(values["amount"]) * 100)
                                 if values.get("amount") else None),
                "amount_raw": values.get("amount") or None,
                "qty": int(values["qty"]) if values.get("qty") else None,
                "memo": values.get("memo") or None,
            })

    # 包内冲突：同一订单在同一个包里出现多次（重复 / 自相矛盾）
    conflicts: list[dict] = []
    occurrences: dict[int, list[dict]] = {}
    for row in rows:
        occurrences.setdefault(row["order_id"], []).append(row)
    for order_id, items in occurrences.items():
        if len(items) > 1:
            ops = "/".join(i["op"] for i in items)
            conflicts.append(_conflict(
                order_id, items[0]["op"], "duplicate_in_package",
                f"订单 {order_id} 在包内出现 {len(items)} 次（{ops}）；每个包内每个订单只能出现一次。",
                line_no=items[0]["line_no"],
                line_nos=[i["line_no"] for i in items],
            ))

    if not errors and not records:
        errors.append(_err(header_line, 0, "", "empty_package", "",
                           "包内没有变更行（只有表头/元数据）。"))
    if not errors and not rows and not conflicts:
        errors.append(_err(header_line, 0, "", "empty_package", "",
                           "包内没有可应用的变更行。"))

    return {
        "package_id": meta.get("package_id") or "",
        "base_version": base_version,
        "note": meta.get("note") or "",
        "format": meta.get("format") or "",
        "header": cells,
        "header_line": header_line,
        "op_count": len(rows),
        "op_counts": {
            "add": sum(1 for r in rows if r["op"] == "add"),
            "reprice": sum(1 for r in rows if r["op"] == "reprice"),
            "cancel": sum(1 for r in rows if r["op"] == "cancel"),
        },
        "rows": rows,
        "errors": errors,
        "conflicts": conflicts,
        "meta": meta,
    }


def _load_snapshot(conn: sqlite3.Connection, version_no: int) -> dict[int, dict]:
    rows = conn.execute(
        "SELECT order_id, site, category, order_date, amount_cents, qty, memo"
        " FROM version_orders WHERE version_no = ?",
        (version_no,),
    ).fetchall()
    return {int(r["order_id"]): dict(r) for r in rows}


def _base_conflicts(base_orders: dict[int, dict], package_rows: list[dict]) -> list[dict]:
    conflicts: list[dict] = []
    for row in package_rows:
        order_id = row["order_id"]
        existing = base_orders.get(order_id)
        if row["op"] == "add":
            if existing is not None:
                conflicts.append(_conflict(
                    order_id, "add", "order_exists",
                    f"新增的订单 {order_id} 在基准版本中已存在；新增只能用于新订单。",
                    line_no=row["line_no"],
                ))
            continue

        if existing is None:
            conflicts.append(_conflict(
                order_id, row["op"], "order_not_found",
                f"{row['op']} 的订单 {order_id} 不在基准版本中；"
                "同一包内不能先 add 再 reprice/cancel。",
                line_no=row["line_no"],
            ))
            continue

        for field in ("site", "category", "order_date"):
            provided = row.get(field)
            if provided and provided != existing[field]:
                conflicts.append(_conflict(
                    order_id, row["op"], "field_mismatch",
                    f"订单 {order_id} 的 {field} 在包内为「{provided}」，"
                    f"基准版本为「{existing[field]}」；包可能基于错误的版本。",
                    line_no=row["line_no"], field=field,
                    expected=existing[field], actual=provided,
                ))
    return conflicts


def _stale_conflicts(conn: sqlite3.Connection, base_version: int | None,
                     current: int | None, package_rows: list[dict]) -> list[dict]:
    """基准过期时，逐个列出自基准版本以来已被其他包改动的订单（只用于说明冲突，不应用）。"""
    if base_version is None or current is None or base_version >= current:
        return []
    changed: dict[int, int] = {}
    for row in conn.execute(
        "SELECT order_id, version_no FROM version_changes"
        " WHERE version_no > ? AND version_no <= ?",
        (base_version, current),
    ).fetchall():
        changed.setdefault(int(row["order_id"]), int(row["version_no"]))
    conflicts: list[dict] = []
    for row in package_rows:
        version_no = changed.get(row["order_id"])
        if version_no is not None:
            conflicts.append(_conflict(
                row["order_id"], row["op"], "changed_after_base",
                f"订单 {row['order_id']} 在基准版本 {base_version} 之后已被版本 {version_no} 改动；"
                "本包基于旧版本，整包拒绝。",
                line_no=row["line_no"], changed_in_version=version_no,
            ))
    return conflicts


def _apply_ops(base_orders: dict[int, dict], package_rows: list[dict]) -> dict[int, dict]:
    snapshot = {oid: dict(row) for oid, row in base_orders.items()}
    for row in sorted(package_rows, key=lambda r: r["order_id"]):
        order_id = row["order_id"]
        if row["op"] == "add":
            snapshot[order_id] = {
                "order_id": order_id,
                "site": row["site"],
                "category": row["category"],
                "order_date": row["order_date"],
                "amount_cents": int(row["amount_cents"]),
                "qty": int(row["qty"]),
                "memo": row["memo"],
            }
        elif row["op"] == "reprice":
            target = snapshot[order_id]
            target["amount_cents"] = int(row["amount_cents"])
            if row.get("qty") is not None:
                target["qty"] = int(row["qty"])
            if row.get("memo") is not None:
                target["memo"] = row["memo"]
        elif row["op"] == "cancel":
            snapshot.pop(order_id, None)
    return snapshot


def _summarize_snapshot(snapshot: dict[int, dict]) -> dict:
    rows = []
    for order in snapshot.values():
        cents = int(order["amount_cents"])
        rows.append({
            "site": order["site"],
            "amount_cents": cents,
            "amount_raw": f"{cents // 100}.{cents % 100:02d}",
            "qty": int(order["qty"]),
        })
    return aggregate.summarize(rows)


def _existing_version(conn: sqlite3.Connection, sha256: str, package_id: str) -> dict | None:
    by_sha = store.find_version_by_sha(conn, sha256)
    if by_sha is not None:
        return {"kind": "sha", "row": by_sha}
    if package_id:
        by_pkg = store.find_version_by_package(conn, package_id)
        if by_pkg is not None:
            return {"kind": "package", "row": by_pkg}
    return None


def _record_reject(conn: sqlite3.Connection, *, filename: str, sha256: str, parsed: dict,
                   status: str, detail: str, conflicts: list[dict] | None = None,
                   errors: list[dict] | None = None, current: int | None = None,
                   encoding_name: str = "") -> dict:
    attempt_id = store.record_change_attempt(
        conn, filename=filename, sha256=sha256, status=status,
        package_id=parsed.get("package_id") or "",
        base_version=parsed.get("base_version"), current_version=current,
        op_count=parsed.get("op_count", 0),
        conflict_count=len(conflicts or []),
        detail=detail, conflicts=conflicts, errors=errors,
        encoding=encoding_name,
    )
    return {
        "status": status,
        "attempt_id": attempt_id,
        "package_id": parsed.get("package_id") or "",
        "base_version": parsed.get("base_version"),
        "current_version": current,
        "op_count": parsed.get("op_count", 0),
        "package_order_ids": [r["order_id"] for r in parsed.get("rows", [])],
        "conflicts": conflicts or [],
        "conflict_order_ids": _conflict_order_ids(conflicts or []),
        "conflict_count": len(conflicts or []),
        "errors": errors or [],
        "detail": detail,
        "written": 0,
        "version_no": None,
    }


def _conflict_order_ids(conflicts: list[dict]) -> list[int]:
    seen: list[int] = []
    for item in conflicts:
        oid = item.get("order_id")
        if isinstance(oid, int) and oid not in seen:
            seen.append(oid)
    return seen


def apply_package(
    conn: sqlite3.Connection,
    *,
    filename: str,
    sha256: str,
    parsed: dict,
    encoding_name: str = "",
    fail_after: int | None = None,
) -> dict:
    """整包校验基准版本 → 原子写新版本快照并切换指针。"""
    current = store.get_current_version(conn)
    package_id = parsed.get("package_id") or f"PKG-{sha256[:12]}"

    # 1) 包自身格式错误：整包拒绝（422）
    if parsed["errors"]:
        detail = (f"变更包未通过校验：{len(parsed['errors'])} 处问题，整包未应用"
                  f"（首处：第 {parsed['errors'][0]['line_no']} 行）。")
        return _record_reject(conn, filename=filename, sha256=sha256, parsed=parsed,
                              status="rejected", detail=detail,
                              errors=parsed["errors"], current=current,
                              encoding_name=encoding_name)

    # 2) 包内订单冲突：与基准版本无关，先拒绝（409）
    if parsed["conflicts"]:
        detail = (f"包内订单冲突：{len(parsed['conflicts'])} 处，整包未应用；"
                  f"冲突订单 {', '.join(str(i) for i in _conflict_order_ids(parsed['conflicts']))}。")
        return _record_reject(conn, filename=filename, sha256=sha256, parsed=parsed,
                              status="conflict", detail=detail,
                              conflicts=parsed["conflicts"], current=current,
                              encoding_name=encoding_name)

    try:
        conn.execute("BEGIN IMMEDIATE")   # 先拿写锁，再读基准，避免两个班次互相覆盖
        existing = _existing_version(conn, sha256, package_id)
        if existing is not None:
            conn.execute("ROLLBACK")
            row = existing["row"]
            if existing["kind"] == "package" and row["sha256"] != sha256:
                conflict = _conflict(
                    None, "", "package_id_conflict",
                    f"package_id {package_id} 已属于另一份内容（sha256 "
                    f"{row['sha256'][:12]}…）；请更换 package_id 后重试。",
                )
                return _record_reject(conn, filename=filename, sha256=sha256, parsed=parsed,
                                      status="conflict",
                                      detail=f"package_id 冲突：{package_id} 已被另一份包占用。",
                                      conflicts=[conflict], current=current,
                                      encoding_name=encoding_name)
            attempt_id = store.record_change_attempt(
                conn, filename=filename, sha256=sha256, status="already_applied",
                package_id=row["package_id"], base_version=row["base_version"],
                current_version=current, op_count=row["op_count"],
                version_no=row["version_no"], encoding=encoding_name,
                detail=(f"同一变更包已应用为版本 {row['version_no']}，"
                        "本次为幂等重试，未重复计入。"),
            )
            return {
                "status": "already_applied",
                "attempt_id": attempt_id,
                "package_id": row["package_id"],
                "base_version": row["base_version"],
                "current_version": current,
                "version_no": row["version_no"],
                "op_count": row["op_count"],
                "row_count": row["row_count"],
                "total_amount": f"{row['total_amount_cents'] // 100}.{row['total_amount_cents'] % 100:02d}",
                "total_qty": row["total_qty"],
                "conflicts": [],
                "conflict_order_ids": [],
                "conflict_count": 0,
                "errors": [],
                "written": 0,
                "detail": f"幂等重试：该包已应用为版本 {row['version_no']}，未重复计入。",
            }

        # 拿到写锁后重新读当前版本：并发提交时以锁内的值为准，避免两个班次各自 +1 互相覆盖
        current = store.get_current_version(conn)

        # 3) 基准版本必须是当前版本；过期 / 超前都整包拒绝
        base_version = parsed["base_version"]
        stale = (base_version is None
                 or (current is None and base_version != 0)
                 or (current is not None and base_version != current))
        if stale:
            conflicts = _stale_conflicts(conn, base_version, current, parsed["rows"])
            if current is not None and base_version is not None and base_version < current:
                status = "stale_base"
                detail = (f"基准版本 {base_version} 已过期（当前为版本 {current}）；"
                          "整包拒绝，请基于当前版本重新生成变更包。")
            elif current is not None and base_version is not None and base_version > current:
                status = "future_base"
                detail = (f"基准版本 {base_version} 超前于当前版本 {current}；"
                          "整包拒绝，请确认包与当前订单版本。")
            else:
                status = "future_base"
                detail = (f"基准版本 {base_version} 不是当前版本"
                          f"（当前为 {current if current is not None else '尚无版本'}）；整包拒绝。")
            if conflicts:
                detail += (f" 冲突订单 "
                           f"{', '.join(str(i) for i in _conflict_order_ids(conflicts))}。")
            conn.execute("ROLLBACK")
            return _record_reject(conn, filename=filename, sha256=sha256, parsed=parsed,
                                  status=status, detail=detail, conflicts=conflicts,
                                  current=current, encoding_name=encoding_name)

        # 4) 与基准快照逐单核对：add 已存在 / reprice、cancel 不存在 / 字段不符
        base_orders = _load_snapshot(conn, base_version) if base_version else {}
        conflicts = _base_conflicts(base_orders, parsed["rows"])
        if conflicts:
            conn.execute("ROLLBACK")
            detail = (f"变更包与基准版本 {base_version} 存在 {len(conflicts)} 处订单冲突，"
                      f"整包未应用；冲突订单 "
                      f"{', '.join(str(i) for i in _conflict_order_ids(conflicts))}。")
            return _record_reject(conn, filename=filename, sha256=sha256, parsed=parsed,
                                  status="conflict", detail=detail, conflicts=conflicts,
                                  current=current, encoding_name=encoding_name)

        # 5) 计算新快照 + 站点汇总，并在同一事务内写版本、明细、汇总、推进指针
        snapshot = _apply_ops(base_orders, parsed["rows"])
        summary = _summarize_snapshot(snapshot)
        if not summary["consistent"]:
            conn.execute("ROLLBACK")
            return _record_reject(
                conn, filename=filename, sha256=sha256, parsed=parsed, status="rolled_back",
                detail="新快照的聚合自检未通过（分组和 / 明细线性 / Decimal 复核不一致），未切换版本。",
                current=current, encoding_name=encoding_name)

        version_no = (current or 0) + 1
        counts = parsed["op_counts"]
        conn.execute(
            "INSERT INTO order_versions(version_no, package_id, base_version, filename,"
            " sha256, op_count, add_count, reprice_count, cancel_count, row_count,"
            " total_amount_cents, total_qty, status, created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (version_no, package_id, base_version, filename, sha256,
             len(parsed["rows"]), counts["add"], counts["reprice"], counts["cancel"],
             len(snapshot), summary["total_amount_cents"], summary["total_qty"],
             "committed", store.now_iso()),
        )
        written = 0
        for order_id in sorted(snapshot):
            order = snapshot[order_id]
            conn.execute(
                "INSERT INTO version_orders(version_no, order_id, site, category,"
                " order_date, amount_cents, qty, memo) VALUES(?,?,?,?,?,?,?,?)",
                (version_no, order_id, order["site"], order["category"],
                 order["order_date"], int(order["amount_cents"]), int(order["qty"]),
                 order.get("memo")),
            )
            written += 1
            if fail_after is not None and written >= fail_after:
                raise RuntimeError(
                    f"注入故障：写入第 {written} 行后中断（用于证明版本切换可回滚）")

        for group in summary["groups"]:
            conn.execute(
                "INSERT INTO version_site_summary(version_no, site, amount_cents,"
                " qty, row_count) VALUES(?,?,?,?,?)",
                (version_no, group["site"], int(group["amount_cents"]),
                 int(group["qty"]), int(group["row_count"])),
            )
        for row in parsed["rows"]:
            conn.execute(
                "INSERT INTO version_changes(version_no, order_id, op) VALUES(?,?,?)",
                (version_no, row["order_id"], row["op"]),
            )

        store.set_current_version(conn, version_no)   # 指针与快照同事务切换
        conn.execute("COMMIT")
    except Exception as exc:  # noqa: BLE001 - 版本切换失败必须整体回滚
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        last = last_complete_version(conn)
        detail = (f"版本切换失败并已回滚：{exc}；当前订单仍停在最后完整版本 "
                  f"{last if last is not None else '（无）'}。")
        return _record_reject(conn, filename=filename, sha256=sha256, parsed=parsed,
                              status="rolled_back", detail=detail,
                              current=last, encoding_name=encoding_name)

    attempt_id = store.record_change_attempt(
        conn, filename=filename, sha256=sha256, status="committed",
        package_id=package_id, base_version=base_version, current_version=version_no,
        op_count=len(parsed["rows"]), version_no=version_no, encoding=encoding_name,
        detail=(f"基于版本 {base_version} 应用 {len(parsed['rows'])} 条变更，"
                f"切换到版本 {version_no}（订单 {len(snapshot)} 行）。"),
    )
    return {
        "status": "committed",
        "attempt_id": attempt_id,
        "package_id": package_id,
        "base_version": base_version,
        "previous_version": current,
        "current_version": version_no,
        "version_no": version_no,
        "op_count": len(parsed["rows"]),
        "op_counts": counts,
        "row_count": len(snapshot),
        "total_amount": summary["total_amount"],
        "total_amount_cents": summary["total_amount_cents"],
        "total_qty": summary["total_qty"],
        "aggregate": summary,
        "conflicts": [],
        "conflict_order_ids": [],
        "conflict_count": 0,
        "errors": [],
        "written": len(parsed["rows"]),
        "snapshot_rows_written": written,
        "detail": (f"基于版本 {base_version} 整包应用 {len(parsed['rows'])} 条变更，"
                   f"当前订单与站点汇总已一次性切换到版本 {version_no}。"),
    }


# ---------------------------------------------------------------- 完整性与恢复


def verify_version(conn: sqlite3.Connection, version_no: int) -> dict:
    """核对某个版本快照是否完整自洽（明细 / 汇总 / 头行三者一致）。"""
    row = conn.execute(
        "SELECT * FROM order_versions WHERE version_no = ?", (version_no,)
    ).fetchone()
    if row is None:
        return {"version_no": version_no, "ok": False, "checks": [
            {"name": "版本存在", "ok": False, "detail": "版本行不存在"},
        ]}

    orders = conn.execute(
        "SELECT COUNT(*) AS c, COALESCE(SUM(amount_cents),0) AS cents,"
        " COALESCE(SUM(qty),0) AS qty FROM version_orders WHERE version_no = ?",
        (version_no,),
    ).fetchone()
    summary = conn.execute(
        "SELECT COUNT(*) AS c, COALESCE(SUM(amount_cents),0) AS cents,"
        " COALESCE(SUM(qty),0) AS qty, COALESCE(SUM(row_count),0) AS rows"
        " FROM version_site_summary WHERE version_no = ?",
        (version_no,),
    ).fetchone()
    by_site = conn.execute(
        "SELECT site, COUNT(*) AS c, SUM(amount_cents) AS cents, SUM(qty) AS qty"
        " FROM version_orders WHERE version_no = ? GROUP BY site",
        (version_no,),
    ).fetchall()
    summary_sites = {
        r["site"]: r for r in conn.execute(
            "SELECT * FROM version_site_summary WHERE version_no = ?", (version_no,)
        ).fetchall()
    }

    site_ok = set(summary_sites) == {r["site"] for r in by_site}
    for r in by_site:
        s = summary_sites.get(r["site"])
        if not s or (s["amount_cents"], s["qty"], s["row_count"]) != (r["cents"], r["qty"], r["c"]):
            site_ok = False
            break

    checks = [
        {"name": "明细行数 == 版本行数",
         "ok": orders["c"] == row["row_count"],
         "detail": f"明细 {orders['c']} 行 / 版本记录 {row['row_count']} 行"},
        {"name": "明细金额和 == 版本金额",
         "ok": orders["cents"] == row["total_amount_cents"],
         "detail": f"明细 {orders['cents']} 分 / 版本 {row['total_amount_cents']} 分"},
        {"name": "明细件数和 == 版本件数",
         "ok": orders["qty"] == row["total_qty"],
         "detail": f"明细 {orders['qty']} 件 / 版本 {row['total_qty']} 件"},
        {"name": "站点汇总与明细逐组一致",
         "ok": site_ok,
         "detail": (f"汇总 {len(summary_sites)} 组 / 明细 {len(by_site)} 组，"
                    f"汇总行数和 {summary['rows']} / 明细 {orders['c']} 行")},
        {"name": "站点汇总合计 == 明细合计",
         "ok": (summary["cents"] == orders["cents"]
                and summary["qty"] == orders["qty"]
                and summary["rows"] == orders["c"]),
         "detail": f"汇总 {summary['cents']} 分 / {summary['qty']} 件；明细 "
                   f"{orders['cents']} 分 / {orders['qty']} 件"},
    ]
    return {"version_no": version_no, "ok": all(c["ok"] for c in checks), "checks": checks}


def last_complete_version(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT MAX(version_no) AS v FROM order_versions").fetchone()
    latest = int(row["v"] or 0)
    for version_no in range(latest, 0, -1):
        if verify_version(conn, version_no)["ok"]:
            return version_no
    return None


def recover_current(conn: sqlite3.Connection) -> dict:
    """把当前指针收敛到最后完整版本。

    正常路径下指针与快照同事务提交，无需修复；这里覆盖手工改库、旧库升级、
    以及极端掉电后指针异常的情况，保证「中断后回到最后完整版本」。
    """
    current = store.get_current_version(conn)
    candidate = last_complete_version(conn)
    if candidate is None:
        return {"recovered": False, "from": current, "to": None,
                "reason": "库中还没有完整版本"}
    current_ok = current is not None and verify_version(conn, current)["ok"]
    if current_ok and current == candidate:
        return {"recovered": False, "from": current, "to": candidate,
                "reason": "当前指针已指向最后完整版本"}
    store.set_current_version(conn, candidate)
    return {
        "recovered": True,
        "from": current,
        "to": candidate,
        "reason": ("当前指针缺失或指向不完整版本" if not current_ok
                   else "存在更新的完整版本，指针已前移到最后完整版本"),
    }


def ensure_baseline_version(conn: sqlite3.Connection) -> dict:
    """首次启动用 seed 快照建立版本 1（幂等）。"""
    if store.get_current_version(conn) is not None or store.max_version_no(conn) > 0:
        return {"created": False, "reason": "已存在版本"}
    from . import settings

    path = settings.SAMPLE_DIR / settings.SEED_FILENAME
    if not path.exists():
        return {"created": False, "reason": f"基线样例缺失：{path.name}"}

    raw = path.read_bytes()
    try:
        probe = encoding.probe(raw)
    except encoding.EncodingError as exc:
        return {"created": False, "reason": f"基线编码失败：{exc.message}"}

    from . import pipeline, rows as rows_mod

    header, data, _ = pipeline.parse_csv(probe.text)
    checked = contract.check(header, "reject")
    if not checked["ok"]:
        return {"created": False, "reason": "基线表头契约未通过"}
    validation = rows_mod.validate(data, checked["header"], checked["columns_used"])
    if not validation.ok:
        return {"created": False, "reason": "基线行校验未通过"}

    snapshot = {
        int(row["order_id"]): {
            "order_id": int(row["order_id"]),
            "site": row["site"],
            "category": row["category"],
            "order_date": row["order_date"],
            "amount_cents": int(row["amount_cents"]),
            "qty": int(row["qty"]),
            "memo": row.get("memo") or None,
        }
        for row in validation.rows
    }
    summary = _summarize_snapshot(snapshot)
    if not summary["consistent"]:
        return {"created": False, "reason": "基线聚合自检未通过"}

    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO order_versions(version_no, package_id, base_version, filename,"
            " sha256, op_count, add_count, reprice_count, cancel_count, row_count,"
            " total_amount_cents, total_qty, status, created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (1, "SEED-BASELINE", 0, settings.SEED_FILENAME,
             hashlib.sha256(raw).hexdigest(),
             len(snapshot), len(snapshot), 0, 0, len(snapshot),
             summary["total_amount_cents"], summary["total_qty"], "baseline",
             store.now_iso()),
        )
        for order_id in sorted(snapshot):
            order = snapshot[order_id]
            conn.execute(
                "INSERT INTO version_orders(version_no, order_id, site, category,"
                " order_date, amount_cents, qty, memo) VALUES(?,?,?,?,?,?,?,?)",
                (1, order_id, order["site"], order["category"], order["order_date"],
                 order["amount_cents"], order["qty"], order.get("memo")),
            )
        for group in summary["groups"]:
            conn.execute(
                "INSERT INTO version_site_summary(version_no, site, amount_cents,"
                " qty, row_count) VALUES(?,?,?,?,?)",
                (1, group["site"], int(group["amount_cents"]), int(group["qty"]),
                 int(group["row_count"])),
            )
        store.set_current_version(conn, 1)
        conn.execute("COMMIT")
    except Exception as exc:  # noqa: BLE001
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        return {"created": False, "reason": f"基线版本写入失败：{exc}"}
    return {"created": True, "version_no": 1, "row_count": len(snapshot)}
