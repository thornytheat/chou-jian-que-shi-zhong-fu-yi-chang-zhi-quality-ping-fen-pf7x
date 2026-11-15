"""剖析流水线：把 6 个业务模块串成一次**可断点续跑**的运行。

顺序固定（缺失 → 重复 → 异常 → 类型/范围 → 评分 → 告警），每一阶段都是纯函数：
输入相同，输出完全相同，因此「已确认的阶段结果」可以落盘，失败后跳过重算。

- ``run`` 支持两种模式：
  * 一次性模式（``checkpoint=None``）：现有行为，供测试与 seed 复用。
  * 断点模式（传入 ``checkpoint_cb``）：每阶段结束后回调保存已确认结果；重跑时把
    ``prior_stages`` 里已确认的阶段直接复用，只补算未完成阶段。
- 未完成的阶段不会出现在最终 report 里：告警/分数/异常组都只由已确认结果拼装，
  中途失败抛出异常，由路由层标记 withheld，不发布任何结果。
"""
from __future__ import annotations

import time

from . import alerts, duplicates, missing, outliers, score, validators
from .loader import missing_tokens

STAGES = [
    ("load", "载入数据与规则"),
    ("missing", "缺失率检查"),
    ("duplicates", "重复检测"),
    ("outliers", "异常值检测（IQR + 稳健z）"),
    ("validation", "类型与范围校验"),
    ("score", "加权评分"),
    ("alerts", "告警合并去重"),
]

# 每个阶段完成后需要落盘的「已确认结果」键
CHECKPOINT_KEYS = {
    "load": ["tokens"],
    "missing": ["missing"],
    "duplicates": ["duplicates"],
    "outliers": ["outliers"],
    "validation": ["validation"],
    "score": ["score"],
    "alerts": ["alerts"],
}

# 依赖关系：某阶段需要的前序产物
STAGE_INPUTS = {
    "load": [],
    "missing": ["tokens"],
    "duplicates": [],
    "outliers": [],
    "validation": ["tokens"],
    "score": ["missing", "duplicates", "outliers", "validation"],
    "alerts": ["missing", "duplicates", "outliers", "validation"],
}


def run(rows: list[dict[str, str]], columns: list[str], rules: dict,
        weights: dict | None = None, progress=None,
        prior_stages: dict | None = None, checkpoint_cb=None) -> dict:
    """执行剖析。``prior_stages`` 为上次已确认的阶段产物，``checkpoint_cb`` 用于落盘。

    prior_stages 形如 ``{"tokens": ..., "missing": {...}, ...}``；
    只有其依赖全部就绪的阶段才会被复用，否则重算。
    """
    state: dict = dict(prior_stages or {})
    stages: list[dict] = []
    started = time.perf_counter()

    def confirmed(stage_name: str) -> bool:
        needed = STAGE_INPUTS[stage_name]
        return all(key in state for key in needed) and all(key in state for key in CHECKPOINT_KEYS[stage_name])

    def emit(index: int, ms: float, resumed: bool) -> None:
        name, label = STAGES[index]
        stages.append({"name": name, "label": label, "ms": round(ms, 2),
                       "resumed": resumed})

    def commit(index: int, elapsed: float, resumed: bool) -> None:
        name = STAGES[index][0]
        emit(index, elapsed, resumed)
        if progress:
            progress(index, STAGES[index][1])
        if checkpoint_cb is not None:
            checkpoint_cb({
                "stage": name,
                "index": index,
                "state": {key: state[key] for key in CHECKPOINT_KEYS[name]},
            })

    def begin(index: int) -> float:
        if progress:
            progress(index, STAGES[index][1], running=True)
        return time.perf_counter()

    # 0) 载入
    t0 = begin(0)
    if confirmed("load"):
        resumed = True
    else:
        resumed = False
        state["tokens"] = missing_tokens(rules)
    commit(0, (time.perf_counter() - t0) * 1000 if not resumed else 0.0, resumed)

    # 1) 缺失
    if confirmed("missing"):
        commit(1, 0.0, True)
    else:
        t = begin(1)
        state["missing"] = missing.analyze(rows, columns, rules)
        commit(1, (time.perf_counter() - t) * 1000, False)

    # 2) 重复
    if confirmed("duplicates"):
        commit(2, 0.0, True)
    else:
        t = begin(2)
        state["duplicates"] = duplicates.analyze(rows, columns, rules)
        commit(2, (time.perf_counter() - t) * 1000, False)

    # 3) 异常
    if confirmed("outliers"):
        commit(3, 0.0, True)
    else:
        t = begin(3)
        state["outliers"] = outliers.analyze(rows, columns, rules)
        commit(3, (time.perf_counter() - t) * 1000, False)

    # 4) 类型/范围
    if confirmed("validation"):
        commit(4, 0.0, True)
    else:
        t = begin(4)
        state["validation"] = validators.analyze(rows, columns, rules, state["tokens"])
        commit(4, (time.perf_counter() - t) * 1000, False)

    # 5) 评分
    if confirmed("score"):
        commit(5, 0.0, True)
    else:
        t = begin(5)
        units = score.build_units(state["missing"], state["duplicates"],
                                  state["outliers"], state["validation"], rules)
        state["score"] = score.compute(units, rules, weights)
        commit(5, (time.perf_counter() - t) * 1000, False)

    # 6) 告警
    if confirmed("alerts"):
        commit(6, 0.0, True)
    else:
        t = begin(6)
        state["alerts"] = alerts.build(state["missing"], state["duplicates"],
                                       state["outliers"], state["validation"])
        commit(6, (time.perf_counter() - t) * 1000, False)

    report = _report(state, rows, columns, rules, stages, started)
    if checkpoint_cb is not None:
        checkpoint_cb({"complete": True, "report": report, "stage": "done", "index": 7})
    return report


def completed_stages(checkpoint: dict | None) -> dict:
    """从落盘 checkpoint 还原已确认的阶段状态；只有 complete=False 的才需要续跑。"""
    if not checkpoint or checkpoint.get("complete"):
        return {}
    return dict(checkpoint.get("state") or {})


def _report(state, rows, columns, rules, stages, started) -> dict:
    return {
        "dataset": {
            "rows": len(rows),
            "columns": len(columns),
            "column_names": columns,
            "rule_version": rules.get("version", "1"),
            "business_key": state["duplicates"]["business_key"],
            "group_by": rules.get("group_by", "category"),
        },
        "missing": state["missing"],
        "duplicates": state["duplicates"],
        "outliers": state["outliers"],
        "validation": state["validation"],
        "score": state["score"],
        "alerts": state["alerts"],
        "stages": stages,
        "elapsed_ms": round((time.perf_counter() - started) * 1000, 2),
        "rules_snapshot": _rules_snapshot(rules),
    }


def _rules_snapshot(rules: dict) -> dict:
    return {
        "version": rules.get("version"),
        "business_key": rules.get("business_key"),
        "group_by": rules.get("group_by"),
        "min_group_sample": rules.get("min_group_sample"),
        "outlier": rules.get("outlier"),
        "grade_thresholds": rules.get("grade_thresholds"),
        "rule_weights": rules.get("rule_weights"),
        "sampling": rules.get("sampling"),
        "columns": {
            name: {
                "type": spec.get("type"),
                "max_missing_rate": spec.get("max_missing_rate"),
                "weight": spec.get("weight"),
                "rules": spec.get("rules"),
                "range": spec.get("range"),
            }
            for name, spec in rules["columns"].items()
        },
    }
