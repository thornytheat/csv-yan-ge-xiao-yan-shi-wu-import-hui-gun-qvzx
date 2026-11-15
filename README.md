# csvgate · CSV 严格校验、事务导入与订单版本切换

上游每天给一份站点订单 CSV，要求「**全对才入库，错了要知道第几行错哪列**」。
本工具把这份人工核对变成可复核的后端模块，任一行非法则整份回滚，库内明细行数不变。

日常增量则走**订单变更包**：包内是同一订单的新增 / 改价 / 取消，并注明基于哪一版当前订单；
校验通过后当前订单与站点汇总一次性切换（原子），基准过期或包内订单冲突则整包拒绝。

使用者：数据平台组入库值班同学。

## 能力

| 模块 | 落地位置 | 做什么 |
|---|---|---|
| 1 上传与编码探测 | `app/encoding.py` | BOM → UTF-8-SIG；严格 UTF-8 失败 → GB18030；都失败 → 400 + 建议。响应记录是否 BOM、前 16 字节摘要、逐次尝试 |
| 2 表头契约 | `app/contract.py` | 必需列缺失=错误；额外列 `reject`/`warn`；列顺序无关；表头去首尾空白；重复列名=错误 |
| 3 行级校验 | `app/rows.py` | 主键严格递增（重复 `duplicate` / 倒序 `out_of_order`，逐行与上一行比较）、int/date/decimal/enum、必填、长度上限、日期真实性 |
| 4 分组聚合 | `app/aggregate.py` | 按 `site` 求和，并用三条独立口径自证一致 |
| 5 事务导入 | `app/importer.py` | 单事务写 批次+明细+聚合；异常即 ROLLBACK；sha256 重复 → `duplicate_file`，可 `force_new_batch` |
| 6 错误报告 | `app/report.py` | 按错误类型汇总 + 逐行依据（行号/列名/原值/原因）+ 修正建议 |
| 7 变更包与版本切换 | `app/changes.py` | 包级校验（包内唯一 / 与基准逐单核对）、基准版本校验、原子切换当前订单+站点汇总、崩溃回滚到最后完整版本、幂等重试 |

编排在 `app/pipeline.py`（`run` 走整份导入，`run_change_package` 走变更包），路由 `app/main.py` 只做参数解析与渲染。

## 核心规则

1. **编码探测**（证据可查）
   - 前 3 字节是 `EF BB BF` → 按 `utf-8-sig` 解码，BOM 剥离；
   - 否则严格 UTF-8；失败再试 `gb18030`（GBK 超集）；
   - 都失败 → HTTP 400，附三次尝试的失败字节偏移与修复建议。
   - 判定是**确定性**的：纯 ASCII 文件一律算 `utf-8`。
2. **行级错误只报第一处**：同一行同一列按「必填 → 长度 → 类型/枚举 → 业务」取第一个失败原因，
   避免一个超长 site 值同时被报成长度错和枚举错、干扰计数。
3. **主键严格递增**：与「上一条主键可解析的行」比较；相等 → `duplicate`，更小 → `out_of_order`，更大 → 通过并更新基线。
4. **日期真实性**：先匹配 `YYYY-MM-DD`，再用 `date.fromisoformat` 判真实存在（`2026-02-30` → `date_invalid`）。
5. **聚合自证**：分组累加 cents、明细单趟线性累加、`Decimal` 原始字符串复核、组内行数和=明细行数，四条必须同时成立。
6. **事务与重复文件**
   - 全对：`BEGIN IMMEDIATE` 写批次 → 明细 → 聚合 → `COMMIT`；中途异常 `ROLLBACK`，明细行数保持写入前值。
   - 同 `sha256` 已入库且未勾选 `force_new_batch` → 返回 `duplicate_file`，写入 0 行，返回既有批号。
   - 勾选 `force_new_batch` → 新建批次（`-F2`/`-F3` 后缀），旧批次保留且 `superseded_by` 指向新批次，
     语义是「同一份文件被有意重灌，重灌痕迹可追溯」，不是覆盖删除。
7. **订单变更包**（`app/changes.py`）
   - 包格式：`#` 元数据（`package_id`、`base_version`）+ 表头 `op,order_id,site,category,order_date,amount,qty,memo`
     + 变更行；`op ∈ {add, reprice, cancel}`。
   - **包级校验**：`add` 必填完整订单字段、`reprice` 必填金额、`cancel` 只能带 `order_id`；同包内 `order_id`
     只能出现一次（重复 / 同单改价+取消 → `duplicate_in_package`），整包拒绝并列出冲突订单。
   - **基准校验**：`base_version` 必须等于当前版本。小于当前 → `stale_base`，大于 → `future_base`，
     都整包拒绝；过期包还会逐单列出「自基准版本以来被其他包改动的订单」（`changed_after_base`），
     避免两个班次先后提交互相覆盖。
   - **与基准快照核对**：`add` 的订单不能已存在（`order_exists`），`reprice`/`cancel` 的订单必须存在
     （`order_not_found`），包内 `site`/`category`/`order_date` 必须与基准一致（`field_mismatch`）。
   - **版本快照**：每个版本是完整自洽的「当前订单 + 站点汇总」（`version_orders` / `version_site_summary`），
     切换只改 `meta.current_order_version` 指针，不做增量修改；旧版本保留可复核。
   - **原子切换**：`BEGIN IMMEDIATE`（先拿写锁再读基准）→ 写版本头 → 明细 → 站点汇总 → 变更记录 → 推进指针 → `COMMIT`。
     任一步失败或进程中断 → `ROLLBACK`，指针仍指向最后完整版本；`/health` 与启动自检会核对并收敛指针。
   - **幂等重试**：同 `sha256` 或同 `package_id` 的包已应用 → `already_applied`，返回既有版本号，不重复计入；
     同 `package_id` 但内容不同 → `package_id_conflict`。

## 启动

```bash
cd batch-20261114b/projects/gb-623
cp .env.example .env          # 可选，默认端口 18212
docker compose up -d --build
curl -s http://127.0.0.1:18212/health
```

- 页面：<http://127.0.0.1:18212/>
- 数据卷：`csvgate-data:/app/data`（SQLite `csvgate.sqlite3`）
- 启动即幂等 seed：`meta.seed_version = seed-2026-11-07.1`，批次 `B-20261107-001`（60 行历史周批次）；
  重复启动不会重复写入。同时以 seed 快照建立订单**版本 1**（基线），变更包从它开始往上叠加。
  **重建数据库**：`docker compose down -v && docker compose up -d --build`。

## 演示步骤

1. `/workbench` 下载 `normal_500.csv` 并上传 → 结论「已入库」，得批号、站点聚合表、明细前 20 行。
2. 上传 `broken.csv` → HTTP 422，结论「整体回滚」，列出 5 条逐行错误（重复主键 / 倒序 / 非法日期 / 超长 / 枚举越界）。
   刷新首页或 `/health` 看 `fact_rows` 未变。
3. 上传 `gbk_120.csv` → 编码探测显示 `gb18030`，120 行入库。
4. 再上传一次 `normal_500.csv` → `duplicate_file`，写入 0 行；到 `/review` 点「验证重复导入」看只读判定。
5. 勾选 `force_new_batch` 再传 `normal_500.csv` → 新批次，旧批次标 superseded。
6. 打开 `/changes`（订单版本与变更包）：当前版本 `v1`（60 行 seed 快照）。
   上传 `change_ok.csv` → 结论「已切换版本 2」，`+1 / ¥1 / ×1`，站点汇总同步变化，订单仍 60 行。
7. 再传一次同一份 `change_ok.csv` → `already_applied`，版本仍是 2、行数不变（幂等重试不重复计入）。
8. 上传 `change_conflict.csv` → HTTP 409，整包拒绝并列出冲突订单 `700003`（包内改价+取消同一单）。
9. 上传 `change_stale.csv`（基于 v1）→ HTTP 409 `stale_base`，当前订单仍是 v2，不会被旧包覆盖。

命令行等价（验收脚本用的就是这些）：

```bash
curl -s http://127.0.0.1:18212/health
curl -s -F "file=@sample_data/normal_500.csv" -F "extra_policy=reject" http://127.0.0.1:18212/api/import
curl -s -o /tmp/broken.json -w '%{http_code}\n' -F "file=@sample_data/broken.csv" http://127.0.0.1:18212/api/import
python3 -c "import json;d=json.load(open('/tmp/broken.json'));print(d['report']['total'],[e['code'] for e in d['report']['rows']])"

# 变更包
curl -s -F "file=@sample_data/change_ok.csv" http://127.0.0.1:18212/api/change-packages
curl -s -F "file=@sample_data/change_ok.csv" http://127.0.0.1:18212/api/change-packages   # already_applied
curl -s -o /tmp/conflict.json -w '%{http_code}\n' -F "file=@sample_data/change_conflict.csv" \
     http://127.0.0.1:18212/api/change-packages                                        # 409
python3 -c "import json;d=json.load(open('/tmp/conflict.json'));print(d['status'],d['conflict_order_ids'])"
curl -s -o /tmp/stale.json -w '%{http_code}\n' -F "file=@sample_data/change_stale.csv" \
     http://127.0.0.1:18212/api/change-packages                                        # 409
python3 -c "import json;d=json.load(open('/tmp/stale.json'));print(d['status'],d['base_version'],d['current_version'])"
curl -s http://127.0.0.1:18212/api/change-versions
```

## API / 页面

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 + 明细行数 + seed 版本 + 当前订单版本 + 快照完整性自检 |
| POST | `/api/import` | multipart：`file`、`extra_policy`、`force_new_batch`；200 已入库/重复文件、400 编码失败、422 校验拒绝 |
| POST | `/workbench` | 页面表单同管道；通过 → 303 跳结果页，拒绝 → 422 直接渲染错误表 |
| POST | `/api/contract-check` | 只检查表头（含 `warn` 策略演练） |
| POST | `/api/replay-check` | 按 sha256 判定「再次上传会不会重复」 |
| POST | `/api/change-packages` | multipart：`file`；200 `committed`/`already_applied`、409 `stale_base`/`future_base`/`conflict`、422 包格式错误、400 编码失败 |
| GET | `/api/change-versions` | 版本列表 + 当前版本 + 统计 + 启动恢复结论 |
| GET | `/api/change-versions/{version_no}` | 单个版本快照 + 完整性自检 |
| GET | `/api/batches` | 批次列表 + 统计 |
| GET | `/api/samples/{name}` | 样例下载（含 `change_ok.csv` / `change_conflict.csv` / `change_stale.csv`） |
| 页面 | `/`、`/workbench`、`/result/{batch_id}`、`/result/attempt/{id}`、`/review` | 首页/工作台/结果/错误明细/历史复核 |
| 页面 | `/changes`、`/changes/version/{v}`、`/changes/attempt/{id}` | 订单版本与变更包/版本快照/包校验依据 |

## 目录

```text
app/            main.py settings.py pipeline.py seed.py changes.py
                encoding.py contract.py rows.py aggregate.py importer.py report.py store.py
                templates/ static/
sample_data/    normal_500.csv gbk_120.csv seed_20261107.csv broken.csv contract.json
                change_ok.csv change_conflict.csv change_stale.csv make_samples.py
tests/          test_encoding.py test_rows.py test_importer.py test_api.py
                test_changes.py test_changes_api.py conftest.py
Dockerfile docker-compose.yml requirements.txt .env.example
```

## 已知限制

- CSV 解析用标准库 `csv.reader`：支持引号包裹字段，但**行号**按「数据行 + 1」换算，
  字段内含换行时行号会偏移（当前样例与常见导出不含换行字段）。
- 编码只覆盖 UTF-8 / UTF-8-BOM / GB18030；UTF-16/UTF-32 会走到 400 并给出提示，不做自动识别。
- 分隔符固定逗号，不做 `;`/`\t` 自动猜测；SJIS、Big5 等会按 GB18030 误判成功的概率存在（GBK 超集覆盖广），
  需要在契约里约定来源编码。
- 单次上传上限 8MB（约 10 万行量级）；不做流式分片。
- 明细与批次不提供删除接口；重灌靠 `force_new_batch` + `superseded_by` 追溯。
- 变更包每个版本存一份完整快照（不是增量链）：单版本容量与全量订单量成正比，
  演示量级（百行）没有问题；更大数据量应改为「基线 + 增量链」或只存变化集的物化视图。
- 版本历史不提供回退接口：旧版本只读保留供复核；如需回退，基于旧版本重新生成一份变更包即可
  （其 `base_version` 必须等于当前版本）。
- `base_version` 与包内订单逐单核对能挡住两个班次并行提交的覆盖，但**不校验整包语义是否等同上游意图**；
  包内去重与基准一致性是硬规则，业务口径仍由上游负责。
