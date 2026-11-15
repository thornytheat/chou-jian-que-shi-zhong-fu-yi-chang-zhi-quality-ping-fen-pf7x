"""运行编排：抽样 → 对账 → 剖析 → 发布，串起可续跑与发布闸门。

职责边界
--------
- ``sampling``：按品类配额生成可重放清单，含候选/入选/替补/未决。
- ``pipeline``：纯计算的 7 阶段，支持从已确认阶段续跑。
- ``store``：持久化批次源快照、抽样快照、对账结果、阶段断点与审计事件。

发布闸门
--------
只有抽样清单与源数据对账齐套（``reconcile.status == "complete"``）才运行评分并发布
分数/异常组/工单；对账不齐套则运行停在 ``blocked``，只保留抽样与对账证据，不产生任何
分数。任何阶段中途抛错都落断点（已确认阶段），可从断点续跑；未完成阶段不进入结果。
"""
from __future__ import annotations

from . import pipeline, sampling as sampling_mod, store
from .loader import load_rules

# 合并后的阶段列表：抽样与对账插在计算前后，发布会单独占一阶段
STAGES = [
    ("load", "载入数据与规则"),
    ("sampling", "按品类配额抽样（可重放）"),
    ("reconcile", "抽样清单与源数据对账"),
    ("missing", "缺失率检查"),
    ("duplicates", "重复检测"),
    ("outliers", "异常值检测（IQR + 稳健z）"),
    ("validation", "类型与范围校验"),
    ("score", "加权评分"),
    ("alerts", "告警合并去重"),
    ("publish", "发布分数 / 异常组 / 工单"),
]
TOTAL_STAGES = len(STAGES)
# pipeline 里的 missing 是第 1 个阶段，对应合并阶段的索引 3
PIPELINE_OFFSET = 2


class BlockedBeforePublish(Exception):
    """对账未齐套：不允许发布分数/异常组/工单。"""

    def __init__(self, reconcile_result: dict):
        self.reconcile_result = reconcile_result
        super().__init__("抽样清单与源数据对账未齐套，已暂停发布")


def stage_list() -> list[dict]:
    return [{"name": name, "label": label} for name, label in STAGES]


def execute(run_id: str, *, resume: bool = False, progress=None) -> dict:
    """执行（或从中断处续跑）一次运行，返回概要。"""
    run = store.get_run(run_id)
    if run is None:
        raise RuntimeError(f"运行 {run_id} 不存在")
    batch_id = run.get("batch_id") or run_id
    batches = store.batch_rows([batch_id])
    batch = batches.get(batch_id)
    if not batch:
        raise RuntimeError(f"批次 {batch_id} 的源数据快照不存在，无法执行或续跑")
    # 开始计算前清空上一轮结果：中途失败不会让旧的「已发布」结果被当成新结果
    store.begin_execution(run_id)
    rows, columns = batch["rows"], batch["columns"]
    rules = load_rules()
    weights = store.weight_overrides()

    checkpoint = _load_checkpoint(run) if resume else None
    prior = pipeline.completed_stages(checkpoint) if checkpoint else {}
    existing_sampling = run.get("sampling")
    if resume and not prior:
        # 没有任何已确认阶段，等同于重新计算
        resume = False
    if not resume:
        store.save_checkpoint(run_id, {"complete": False, "state": {}})

    def report(index: int, *, running: bool = False) -> None:
        if progress:
            progress(index, STAGES[index][1], running=running)

    # 0) 载入
    report(0)
    store.set_progress(run_id, 0, STAGES[0][1])

    # 1) 抽样：已有快照（续跑 / 再发布）直接复用，避免改变已确认清单
    sampling = existing_sampling
    if sampling is None:
        report(1, running=True)
        store.set_progress(run_id, 1, STAGES[1][1])
        plan = sampling_mod.plan_from_rules(
            rules, run.get("quota_overrides") or {},
            seed=run.get("sampling_seed"),
        )
        supply = store.batch_supply(run_id, limit_runs=10, exclude_batch_id=batch_id)
        sampling = sampling_mod.select(rows, columns, plan, batch_id, supply)
        store.save_sampling(run_id, sampling, None)
    else:
        # 抽样快照是唯一事实来源：重发布的计划/配额以快照为准（撤回/重规划已写回快照）
        sampling_mod.recount(sampling)
        store.set_progress(run_id, 1, STAGES[1][1])

    # 2) 对账（跨批来源批次也要能定位并校验指纹）
    report(2, running=True)
    store.set_progress(run_id, 2, STAGES[2][1])
    external = _external_batches(sampling, batch_id)
    events = store.list_audit_events(run_id)
    reconcile_result = sampling_mod.reconcile(
        sampling, batch_id, rows, columns, sampling["fingerprint"],
        external_batches=external, events=events, at=store.now(),
    )
    store.save_sampling(run_id, sampling, reconcile_result)
    if reconcile_result["status"] != "complete":
        store.set_run_state(run_id, "blocked", STAGES[2][1], 2,
                            error="抽样清单与源数据对账未齐套：" + "；".join(
                                reconcile_result["blockers"][:3]))
        return _summary(run_id, sampling, reconcile_result, published=False)
    store.set_publish_status(run_id, "republish_required")
    store.set_run_state(run_id, "running", STAGES[3][1], 3)

    # 3-9) 剖析 + 发布：仅对入选行评分
    sampled_rows = selected_rows(sampling, columns)
    report(3, running=True)
    store.set_progress(run_id, 3, STAGES[3][1])

    state: dict = dict(prior)

    def checkpoint_cb(checkpoint_payload: dict) -> None:
        if checkpoint_payload.get("complete"):
            return
        state.update(checkpoint_payload.get("state") or {})
        index = checkpoint_payload.get("index", 0)
        if index >= 1:  # 跳过 pipeline 自己的 load 阶段（合并阶段已有）
            combined = index + PIPELINE_OFFSET
            store.set_progress(run_id, combined, STAGES[combined][1])
        store.save_checkpoint(run_id, {
            "complete": False,
            "stage": checkpoint_payload.get("stage"),
            "state": state,
            "sampling_ready": True,
        })

    def pipeline_progress(index: int, label: str, running: bool = False) -> None:
        if index >= 1:
            combined = index + PIPELINE_OFFSET
            store.set_progress(run_id, combined, STAGES[combined][1])

    report_obj = pipeline.run(
        sampled_rows, columns, rules, weights,
        progress=pipeline_progress,
        prior_stages=prior,
        checkpoint_cb=checkpoint_cb,
    )
    report_obj["dataset"].update({
        "rows": len(rows),
        "sampled_rows": len(sampled_rows),
        "source_rows": len(rows),
        "batch_id": batch_id,
        "source": run["source"],
        "encoding": (run.get("report") or {}).get("dataset", {}).get("encoding", "utf-8"),
    })
    report_obj["run_id"] = run_id

    report(TOTAL_STAGES - 1, running=True)
    store.set_progress(run_id, TOTAL_STAGES - 1, STAGES[-1][1])
    store.save_checkpoint(run_id, {"complete": True, "report": report_obj})
    store.finish_run(run_id, report_obj, sampling=sampling, reconcile_result=reconcile_result)
    return _summary(run_id, sampling, reconcile_result, published=True)


def recompute(run_id: str, *, progress=None) -> dict:
    """抽样清单变更（撤回 / 重规划）后，按最新清单重算并重新对账、重新发布。"""
    run = store.get_run(run_id)
    if run is None:
        raise RuntimeError(f"运行 {run_id} 不存在")
    store.save_checkpoint(run_id, {"complete": False, "state": {}})
    store.set_run_state(run_id, "running", "重新对账", 2)
    return execute(run_id, resume=False, progress=progress)


def selected_rows(sampling: dict, columns: list[str]) -> list[dict]:
    """取入选行（含跨批补录行），按抽样顺序输出；未决占位不参与计算。"""
    rows = []
    for entry in sampling["entries"]:
        if entry["status"] != sampling_mod.SELECTED:
            continue
        row = entry.get("row") or {"row_id": entry["row_id"], "category": entry["category"]}
        rows.append({column: str(row.get(column, "")) for column in columns})
    return rows


def _external_batches(sampling: dict, current_batch_id: str) -> dict:
    source_batches = sorted({
        entry["source_batch"] for entry in sampling["entries"]
        if entry.get("source_batch") and entry["source_batch"] != current_batch_id
    })
    if not source_batches:
        return {}
    return store.batch_rows(source_batches)


def _load_checkpoint(run: dict) -> dict | None:
    checkpoint = run.get("checkpoint")
    return checkpoint if isinstance(checkpoint, dict) else None



def _summary(run_id: str, sampling: dict, reconcile_result: dict, *,
             published: bool) -> dict:
    counts = sampling.get("counts") or {}
    return {
        "run_id": run_id,
        "published": published,
        "sampling": {
            "selected": counts.get("selected", 0),
            "replacement": counts.get("replacement", 0),
            "candidate": counts.get("candidate_backlog", 0),
            "pending": counts.get("pending_slots", 0),
            "withdrawn": counts.get("withdrawn", 0),
            "cross_batch_in": counts.get("cross_batch_in", 0),
        },
        "reconcile": reconcile_result,
    }
