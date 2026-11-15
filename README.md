# qualiscan · 抽检缺失重复异常值质量评分

上游商品主数据的月度抽检工具：先按**品类配额生成可重放抽样清单**并与源数据对账，齐套后才把入选行交给规则集，
真算缺失率、复合键/整行重复、按品类分组的双法异常值，
输出**可复现的百分制分数 + A/B/C/D 分级**，并把同一单元格的多条命中**合并**成一张可直接下发的整改工单。

- 使用者：数据质量组
- 输入：`sample_data/quality_sample.csv`（2000 行 × 9 列、5 个品类）或上传自己的 CSV + `sample_data/rules.json`
- 输出：抽样清单（候选/入选/替补/未决/撤回）、对账结果、评分卡、high/medium 告警、工单 CSV、审计事件

## 一、能力（抽样闸门 + 6 个业务模块）

| 模块 | 落地 | 说明 |
|---|---|---|
| 品类配额抽样 | `app/sampling.py` | 按品类配额生成**可重放**清单：入选按 `sha256(seed\|batch\|row_id)` 升序取前 N，不依赖行序与随机数状态 |
| 候选不足 / 跨批补录 | `app/sampling.py` | 本批候选不足 → 从既往批次候选池跨批补录并写明来源批次；仍不足 → `pending` 未决占位 |
| 超配与撤回 | `app/sampling.py` | 重规划下调配额 → 超配行按顺位转入替补池/退回来源批次；撤回保留操作人/原因/时间，替补顶上 |
| 抽样对账闸门 | `app/sampling.py` + `app/orchestrate.py` | 逐行校验 row_id/品类/行哈希/来源指纹/配额/撤回审计；全部通过才发布分数、异常组与工单 |
| 断点续跑 | `app/pipeline.py` + `app/orchestrate.py` | 每阶段落「已确认结果」，失败/中断后从断点续跑，未完成阶段不进入结果 |
| 规则集 | `app/loader.py` + `sample_data/rules.json` | 列类型、允许缺失率、权重、取值范围、分组列、阈值、抽样配额；上传时严格校验表头 |
| 数据剖析 | `app/pipeline.py` | 固定 7 阶段流水线，每阶段耗时与产物都可追溯 |
| 重复检测 | `app/duplicates.py` | 复合业务键重复与整行重复分开统计，输出重复组 |
| 异常检测 | `app/outliers.py` | 按品类分组，IQR(1.5×IQR) 与稳健 z(>3.5) 双法，样本 < 8 不判 |
| 评分卡 | `app/score.py` | 每列每规则 `1 − 违规率` 裁剪到 [0,1]，按权重归一为百分制 + 分级 |
| 工单导出 | `app/alerts.py` + `app/export.py` | 同一 `(row_id, column)` 多规则合并一条，`rules[]` 保留集合，导出 CSV |

页面 5 个：`/`（场景、演示步骤、规则集概览）、`/workbench`（样例/上传/粘贴 + 配额覆盖 + 抽样状态板 + 分阶段进度）、`/result/{run_id}`（发布结果：分数、违规率表、告警依据、抽样与对账）、`/result/{run_id}`（未齐套时显示未发布与对账明细）、`/review`（历史、权重编辑与重算、工单下载）。

## 二、核心规则

0. **抽样闸门**：每次运行先按 `rules.sampling` 的品类配额抽样，再与源数据对账；只有 `reconcile.status=complete`
   才运行评分并发布分数/异常组/工单。对账不齐套 → 运行停在 `blocked`、`publish_status=withheld`，不产生任何分数。
1. **缺失**：按列缺失率 `missing/total` 与规则 `max_missing_rate` 比较；`allowed`（未超允许，不扣分）/ `violation`（超了）/ `all_empty`（整列全空，最严重）。缺失值识别含 `NA`/`NULL`/`-` 等占位符。
2. **重复**：`business_key`（默认 `product_id + category`）重复 → `duplicate_key`，输出每组行号与额外行数；除 `row_id` 外所有列一致 → `duplicate_row`。整行重复整体归入 `duplicate_row`，不再重复计入 `duplicate_key`（避免同一批行双重扣分）。
3. **异常值**：按 `group_by`（品类）分组；组内有效样本 < `min_group_sample`(8) → `insufficient_sample`，整组不判。IQR 法（Q1−1.5·IQR / Q3+1.5·IQR，线性插值分位）与稳健 z（`|x−median|/(1.4826·MAD) > 3.5`）双法：**双命中 high、单命中 medium**；`MAD = 0` 时稳健 z 不可判（不误报，标注 `mad_zero`）。
4. **评分**：`score = 1 − 违规率` 裁剪到 `[0,1]`；单元权重 = 列权重 × 规则权重；`总分 = 100 × Σ(score×w) / Σw`；`A ≥ 90 / B ≥ 80 / C ≥ 70 / D < 70`。规则无有效样本时按满分计入并在结果页注明。**只对抽样入选行评分**。
5. **告警去重**：同一 `(row_id, column)` 只保留一条，`rules[]` 为全部命中规则，`severity` 取最严重一档；缺失违规列按「前 20 行逐行 + 其余按列汇总」输出，避免单列 300 行把工单撑爆。

### 抽样口径细节

- **可重放**：入选只由 `sha256(seed|batch_id|row_id)` 升序决定；`replay` 会按存储的计划/种子/候选池快照重算，
  并按审计操作（撤回/重规划）重放一遍，逐行比对。批次号默认由 `种子 + 数据源 + 源指纹` 派生——同一输入再跑得到同一清单；
  月度分批次时可显式传 `batch` 标签。
- **候选 / 入选 / 替补 / 未决**：配额内入选 → 其后 `replacement_pool` 行入替补池（标注第 i 顺位）→ 其余为候选；
  候选不足先跨批补录，仍不足生成 `pending` 占位（`missing_slots` 计缺口）。
- **跨批补录**：来自既往批次候选池，每行写明来源批次与原顺位；对账会校验来源批次源数据指纹未漂移。
- **超配**：`replan` 下调配额后，超出新配额的入选行按顺位转入替补池（本批行）或退回来源批次候选池（跨批行），
  每一行去向写入 `disposition` 与审计事件；`over_quota_allowance` 可保留少量超配。
- **撤回**：只允许入选/替补行；必须写操作人与原因；撤回后同品类替补顺位最高者顶上、空位再由候选补满，
  替补池为空则跨批补录、仍无则记未决。撤回事件只增不改，永久保留在 `audit_events`。
- **对账项**：源数据指纹一致、清单行可定位、品类一致、行哈希一致、跨批来源可定位且未漂移、入选无重复、
  每个品类配额配齐（含容忍）、撤回行均有审计事件。任一失败即 `mismatch`。
- **断点续跑**：每个阶段（缺失/重复/异常/类型范围/评分/告警）完成后保存已确认结果；失败或中断后可从断点续跑，
  只补算未完成阶段；未完成阶段不进入结果，也不发布。

## 三、启动

```bash
cd projects/gb-630
docker compose config --quiet
docker compose up -d --build
docker compose ps                       # 等 healthy
curl -s http://127.0.0.1:18219/health
```

打开 <http://127.0.0.1:18219/>。数据落在命名卷 `qualiscan-data:/app/data`（SQLite `qualiscan.db`）。
首次启动幂等 seed 一次样例剖析（`settings.seed_version`，重复启动不产生脏数据）。
想重建库：`docker compose down -v` 后再 `up -d`。

容器内跑测试：

```bash
docker compose run --rm app python -m pytest -q
```

## 四、演示步骤

1. 首页看场景、规则集概览、样例埋点说明与抽样闸门口径。
2. `/workbench` 点「执行剖析（样例数据）」→ 页面按 10 阶段显示进度（载入 → 抽样 → 对账 → 缺失 → 重复 → 异常 → 类型范围 → 评分 → 告警 → 发布）→ 自动跳结果页。
3. 结果页：总分与分级、每列每规则违规率表、缺失明细、重复组、异常值分组统计、告警表；「抽样与对账」分段查看候选/入选/替补/未决/撤回、品类配额与对账项，并可撤回行或调整配额。
4. `/review`：改列权重 → 保存 →「按当前权重重算最近一次运行」→ 分数立刻变化；下载工单 CSV。
5. 错误态：上传缺列 CSV（例如删掉 `price`），返回 400 与中文原因，页面可修正后重试。
6. 闸门演示：把某品类配额设得高于该品类行数（例如 `{"孤品": 5}` 且只有 2 行）→ 运行停在「未发布」，结果页显示未决与对账缺口；
   调整配额或补录后点「重新对账并发布」即可发布。

## 五、API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查（含运行数、规则版本） |
| GET | `/api/rules` | 规则集原文 |
| POST | `/api/runs` | `{"source":"sample","category_quota":{...},"batch":"2026-11"}` 用样例执行，返回 `run_id`（后台线程跑） |
| POST | `/api/runs/upload` | multipart 上传 CSV（UTF-8/BOM/GB18030 自动识别），可选 `category_quota` JSON、`batch`、`seed` |
| POST | `/api/runs/text` | `{"csv_text": "...", "category_quota": {...}}` 粘贴执行 |
| GET | `/api/runs/{id}/status` | 进度：`status/stage/...` + `sampling_counts/reconcile_status/publish_status/pending_slots` |
| GET | `/api/runs/{id}/sampling` | 抽样清单分页与汇总（`status=selected,replacement,...` 过滤） |
| GET | `/api/runs/{id}/sampling.csv` | 抽样清单 CSV（状态、替补顺位、来源批次、行哈希、去向说明） |
| POST | `/api/runs/{id}/sampling/replay` | 按计划/种子/候选池快照重放，逐行比对是否一致 |
| POST | `/api/runs/{id}/sampling/withdraw` | `{"row_id","actor","reason"}` 撤回；替代顺位顶上，写审计事件 |
| POST | `/api/runs/{id}/sampling/replan` | `{"category_quota":{...},"actor"}` 调整配额；超配行转替补/退回来源批次 |
| GET | `/api/runs/{id}/reconcile` | 对账结果、缺口与发布状态 |
| GET | `/api/runs/{id}/audit` | 审计事件（撤回/替补/超配/补录等，只增不改） |
| POST | `/api/runs/{id}/resume` | 从已确认阶段续跑；未完成阶段不进入结果 |
| POST | `/api/runs/{id}/publish` | 重新对账；齐套后重算并发布，未齐套保持 withheld |
| GET | `/api/runs/{id}/alerts.csv` | 工单 CSV（UTF-8 BOM）；未发布返回 409 |
| GET | `/api/runs` | 历史运行列表（含抽样/对账/发布状态） |
| GET/POST | `/api/rules/weights` | 读取/保存权重覆盖（0~10，超范围 400） |
| POST | `/api/rules/weights/reset` | 恢复默认权重 |
| POST | `/api/runs/{id}/rescore` | 按当前权重重算该次运行总分（仅已发布可重算） |

错误一律 4xx + `{"detail": "中文原因"}`：缺列、含未定义列、表头重复、空文件、只有表头、超 8MB/2 万行、未知运行 404、运行未完成/未齐套 409、批次标签重复且数据不同 409。

## 六、目录

```
app/
  main.py         路由（薄）
  settings.py     路径与常量
  sampling.py     品类配额抽样（可重放）、替补/跨批补录、超配、撤回、对账、重放
  orchestrate.py  运行编排：抽样 → 对账闸门 → 剖析 → 发布；断点续跑
  loader.py       规则读取、CSV 解析、编码探测、表头契约
  missing.py      缺失规则
  duplicates.py   重复规则
  outliers.py     异常值规则（IQR + 稳健 z）
  validators.py   类型与范围校验
  score.py        评分卡
  alerts.py       告警合并去重
  export.py       工单 / 抽样清单 CSV
  pipeline.py     7 阶段流水线（支持从已确认阶段续跑）
  store.py        SQLite（runs / run_batches / audit_events / settings）
  seed.py         幂等 seed（走完整抽样+对账+发布）
  templates/      base/index/workbench/running/blocked/result/review
  static/         app.js / style.css
sample_data/      quality_sample.csv、rules.json、_generate.py（固定种子可重生成）
tests/            test_missing / test_duplicates / test_outliers / test_score / test_alerts /
                  test_sampling（抽样口径） / test_publish（闸门/撤回/重规划/续跑） / test_api
```

## 七、已知限制

- 评分只覆盖抽样入选行（默认按品类配额共约 50 行），不是全量 2000 行；未入选行不进入分数、异常组或工单。
  需要全量口径时把 `rules.sampling` 的品类配额调到与行数一致。
- 抽样可重放依赖 `seed` + `batch_id` + 行 `row_id`；若上游改了 `row_id`，即为新数据，会得到不同清单（源数据指纹也会变）。
- 评分是加权平均，A 级不等于没有问题：总分 93 时仍可能有上百个单元格命中规则，需按工单整改；分级与告警数是两个维度。
- 重复检测的整行签名按原始字符串比较，`0100` 与 `100`、`1.20` 与 `1.2` 视为不同；未做数值归一化（避免误合并）。
- 异常值以「品类」为唯一天然分组；不处理多列联合异常（如 price 与 weight 的比例异常）。
- MAD = 0 的组只做 IQR 判定；若 IQR 也为 0（全部取值相同），该组不会产生异常，仅记录 `mad_zero`。
- 跨批补录只从**其他批次**的候选池取行；同批次重复运行（确定性批次号）不会把本批候选当作跨批来源。
- 撤回只允许入选/替补行；已撤回行不能重复撤回（审计事件已保留）；撤回后若替补池为空且无跨批候选，会留未决。
- 未做登录/权限；撤回人由操作时填写，审计事件只增不改。上传文件不落盘，只在内存中解析（≤8MB），但批次源快照会写入 SQLite 供对账与重放。
- 后台计算为进程内线程，未引入队列；单实例部署（`docker compose` 一个服务）。断点续跑为手动触发，不自动重试。
- 缺失违规列只逐行列出前 20 行，其余按列汇总；如需逐行完整清单请导出 CSV 后自行筛选。
