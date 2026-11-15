"""FastAPI 路由（薄）：参数解析 + 调用剖析流水线 + 渲染页面。

核心计算都在 missing / duplicates / outliers / score / alerts / validators 里，
路由只负责：取输入、起后台线程、读 SQLite 快照、套模板。
"""
from __future__ import annotations

import json
import pathlib
import threading
import uuid

from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import (export, loader, orchestrate, sampling as sampling_mod,
               score, seed, settings, store)

BASE_DIR = pathlib.Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.filters["pct"] = lambda v, d=1: "—" if v is None else f"{float(v) * 100:.{d}f}%"
templates.env.filters["num"] = lambda v, d=2: "—" if v is None else f"{float(v):.{d}f}"
templates.env.filters["sev"] = lambda s: {"high": "err", "medium": "warn", "low": ""}.get(s, "")

@asynccontextmanager
async def lifespan(_: FastAPI):
    store.connect()
    ensure_seeded()
    yield


app = FastAPI(title=settings.APP_NAME, lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

ALERT_PAGE_LIMIT = 80
_STATE: dict = {"rules": None, "seeded": False}
_LOCK = threading.Lock()


def rules() -> dict:
    if _STATE["rules"] is None:
        _STATE["rules"] = loader.load_rules()
    return _STATE["rules"]


def ensure_seeded() -> None:
    if _STATE["seeded"]:
        return
    with _LOCK:
        if _STATE["seeded"]:
            return
        seed.ensure_seed()
        _STATE["seeded"] = True


# ---------------------------------------------------------------- 页面

def column_specs(r: dict) -> list[dict]:
    """把 rules.json 的列定义投影成模板可直接渲染的只读结构（不含任何计算）。"""
    return [
        {
            "name": name,
            "label": spec.get("label", name),
            "type": spec.get("type"),
            "weight": spec.get("weight"),
            "max_missing_rate": spec.get("max_missing_rate"),
            "rules": [score.RULE_LABELS[x] for x in spec.get("rules", [])],
            "range": spec.get("range"),
        }
        for name, spec in r["columns"].items()
    ]



@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    ensure_seeded()
    r = rules()
    history = store.list_runs(limit=5)
    latest = history[0] if history else None
    latest_report = (store.get_run(latest["run_id"]) or {}).get("report") if latest else None
    latest_report = latest_report or {}
    # 首页统计：只读最近一次运行的报告快照，没有运行记录时全部为 None
    stats = {
        "runs": store.count_runs(),
        "total": latest.get("total_score") if latest else None,
        "grade": latest.get("grade") if latest else None,
        "alerts": (latest_report.get("alerts") or {}).get("total"),
        "high": (latest_report.get("alerts") or {}).get("high"),
        "medium": (latest_report.get("alerts") or {}).get("medium"),
        "merged_cells": (latest_report.get("alerts") or {}).get("merged_cells"),
        "duplicate_groups": ((latest_report.get("duplicates") or {}).get("duplicate_key") or {}).get("group_count"),
        "row_groups": ((latest_report.get("duplicates") or {}).get("duplicate_row") or {}).get("group_count"),
        "outlier_cells": len((latest_report.get("outliers") or {}).get("cells") or []),
        "insufficient": len((latest_report.get("outliers") or {}).get("insufficient") or []),
    }
    return templates.TemplateResponse(request, "index.html", {
        "app_name": settings.APP_NAME,
        "page": "index",
        "rules": r,
        "columns": column_specs(r),
        "history": history,
        "latest": latest,
        "latest_report": latest_report,
        "stats": stats,
        "business_key": r.get("business_key"),
        "rule_weights": r.get("rule_weights", {}),
        "rule_labels": score.RULE_LABELS,
        "thresholds": r.get("grade_thresholds"),
        "outlier": r.get("outlier"),
        "min_group_sample": r.get("min_group_sample"),
        "sample_rows": 2000,
    })


@app.get("/workbench", response_class=HTMLResponse)
def workbench(request: Request):
    ensure_seeded()
    r = rules()
    history = store.list_runs(limit=5)
    return templates.TemplateResponse(request, "workbench.html", {
        "app_name": settings.APP_NAME,
        "page": "workbench",
        "columns": list(r["columns"].keys()),
        "column_specs": column_specs(r),
        "rule_weights": r.get("rule_weights", {}),
        "rule_labels": score.RULE_LABELS,
        "rules_version": r.get("version"),
        "business_key": r.get("business_key"),
        "group_by": r.get("group_by"),
        "outlier": r.get("outlier"),
        "min_group_sample": r.get("min_group_sample"),
        "thresholds": r.get("grade_thresholds"),
        "sample_rows": 2000,
        "history": history,
        "latest": history[0] if history else None,
        "stages": [label for _, label in orchestrate.STAGES],
        "stage_total": orchestrate.TOTAL_STAGES,
        "sampling_plan": sampling_mod.plan_from_rules(r),
    })


@app.get("/result/{run_id}", response_class=HTMLResponse)
def result(request: Request, run_id: str, limit: int = ALERT_PAGE_LIMIT):
    ensure_seeded()
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {run_id} 不存在，请回到工作台重新执行。")
    if run["status"] == "running":
        return templates.TemplateResponse(request, "running.html", {
            "app_name": settings.APP_NAME,
            "page": "result",
            "run": run,
            "stages_total": orchestrate.TOTAL_STAGES,
        })
    if run["status"] in ("failed", "blocked"):
        return templates.TemplateResponse(request, "blocked.html", {
            "app_name": settings.APP_NAME,
            "page": "result",
            "run": run,
            "stages_total": orchestrate.TOTAL_STAGES,
        })
    if run.get("publish_status") != "published" or not run.get("report"):
        return templates.TemplateResponse(request, "blocked.html", {
            "app_name": settings.APP_NAME,
            "page": "result",
            "run": run,
            "stages_total": orchestrate.TOTAL_STAGES,
        })
    report = run["report"]
    total_alerts = report["alerts"]["total"]
    limit = max(10, min(500, limit))
    return templates.TemplateResponse(request, "result.html", {
        "app_name": settings.APP_NAME,
        "page": "result",
        "run": run,
        "report": report,
        "sampling": run.get("sampling"),
        "reconcile": run.get("reconcile"),
        "columns": report["dataset"]["column_names"],
        "score": report["score"],
        "alerts": report["alerts"]["items"][:limit],
        "alerts_shown": min(limit, total_alerts),
        "alerts_total": total_alerts,
        "alert_limit": limit,
        "insufficient": report["outliers"]["insufficient"],
        "rule_labels": score.RULE_LABELS,
        "severity_labels": {"high": "高", "medium": "中", "low": "低"},
    })


@app.get("/review", response_class=HTMLResponse)
def review(request: Request):
    ensure_seeded()
    r = rules()
    overrides = store.weight_overrides()
    columns, rule_weights = score.merged_weights(r, overrides)
    history = store.list_runs(limit=20)
    recent = history[0] if history else None
    return templates.TemplateResponse(request, "review.html", {
        "app_name": settings.APP_NAME,
        "page": "review",
        "rules": r,
        "history": history,
        "recent": recent,
        "columns": [
            {"name": n, "label": s.get("label", n), "default": float(s.get("weight", 1.0)),
             "value": columns[n]}
            for n, s in r["columns"].items()
        ],
        "rule_weights": [
            {"name": k, "label": score.RULE_LABELS.get(k, k), "default": float(v),
             "value": rule_weights[k]}
            for k, v in rule_weights.items()
        ],
        "has_overrides": bool(overrides.get("columns") or overrides.get("rule_weights")),
        "override_count": len(overrides.get("columns") or {}) + len(overrides.get("rule_weights") or {}),
        "thresholds": r.get("grade_thresholds"),
    })


# ---------------------------------------------------------------- API


def _start_run(source: str, data: dict, quota_overrides: dict | None = None,
               sampling_seed: str | None = None, batch_label: str | None = None) -> dict:
    r = rules()
    overrides = store.weight_overrides()
    run_id = uuid.uuid4().hex[:12]
    rows, columns = data["rows"], data["columns"]
    fp = sampling_mod.fingerprint(rows, columns)
    seed = sampling_seed or str((r.get("sampling") or {}).get("seed") or "qualiscan-default")
    batch_id = sampling_mod.derive_batch_id(seed, source, fp, batch_label)
    if not store.save_batch(batch_id, rows, columns, fp):
        raise HTTPException(
            status_code=409,
            detail=f"批次 {batch_id} 已存在且源数据指纹不同；请换一个批次标签，避免跨批混用。",
        )
    store.create_run(run_id, source, overrides, batch_id=batch_id,
                     quota_overrides=quota_overrides, sampling_seed=seed)

    def worker() -> None:
        try:
            def progress(index: int, label: str, running: bool = False) -> None:
                store.set_progress(run_id, index, label)

            orchestrate.execute(run_id, progress=progress)
        except Exception as exc:  # noqa: BLE001 - 记录失败原因给页面看
            store.fail_run(run_id, f"{type(exc).__name__}: {exc}")

    threading.Thread(target=worker, name=f"run-{run_id}", daemon=True).start()
    return {
        "run_id": run_id,
        "batch_id": batch_id,
        "status": "running",
        "stages": [stage["label"] for stage in orchestrate.stage_list()],
    }


@app.post("/api/runs")
def create_run(payload: dict) -> dict:
    ensure_seeded()
    payload = payload or {}
    source = payload.get("source", "sample")
    if source != "sample":
        raise HTTPException(status_code=400, detail="source 只支持 sample；上传请使用 /api/runs/upload。")
    r = rules()
    try:
        data = loader.load_sample(r)
    except loader.InputError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    quotas = _parse_quotas(payload.get("category_quota"))
    return _start_run(data["source"], data, quotas, payload.get("seed"),
                      _clean_label(payload.get("batch")))


def _clean_label(raw) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if len(text) > 64 or any(c in text for c in "/\\|"):
        raise HTTPException(status_code=400, detail="批次标签需为 1~64 字符且不含 / \\ |。")
    return text


@app.post("/api/runs/upload")
async def create_run_upload(file: UploadFile = File(...), category_quota: str | None = None,
                            seed: str | None = None, batch: str | None = None) -> dict:
    ensure_seeded()
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="上传的文件为空，请选择含表头与数据行的 CSV。")
    if len(raw) > settings.MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=400, detail="文件超过 8MB 上限，请分批抽检。")
    r = rules()
    try:
        text, encoding = loader.decode_bytes(raw)
        data = loader.load_csv_text(text, r, f"上传文件 {file.filename or 'upload.csv'}")
    except loader.InputError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    data["encoding"] = encoding
    quotas = _parse_quotas(category_quota)
    return _start_run(data["source"], data, quotas, seed, _clean_label(batch))


@app.post("/api/runs/text")
def create_run_text(payload: dict) -> dict:
    """粘贴 CSV 文本执行（同样走严格表头校验）。"""
    ensure_seeded()
    payload = payload or {}
    text = payload.get("csv_text") or ""
    if not text.strip():
        raise HTTPException(status_code=400, detail="CSV 文本为空，请粘贴含表头的内容。")
    r = rules()
    try:
        data = loader.load_csv_text(text, r, "粘贴的 CSV 文本")
    except loader.InputError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    quotas = _parse_quotas(payload.get("category_quota"))
    return _start_run(data["source"], data, quotas, payload.get("seed"),
                      _clean_label(payload.get("batch")))


def _parse_quotas(raw) -> dict:
    if raw in (None, ""):
        return {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="category_quota 必须是 JSON 对象。") from exc
    if not isinstance(raw, dict):
        raise HTTPException(status_code=400, detail="category_quota 必须是「品类: 配额」对象。")
    quotas = {}
    for name, value in raw.items():
        try:
            quota = int(value)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=f"品类 {name} 的配额必须是整数。") from exc
        if quota < 0:
            raise HTTPException(status_code=400, detail=f"品类 {name} 的配额不能为负。")
        quotas[str(name)] = quota
    return quotas


@app.get("/api/runs/{run_id}/status")
def run_status(run_id: str) -> dict:
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {run_id} 不存在。")
    stages = orchestrate.stage_list()
    report = run.get("report") or {}
    sampling = run.get("sampling") or {}
    counts = sampling.get("counts") or {}
    reconcile_result = run.get("reconcile") or {}
    return {
        "run_id": run_id,
        "batch_id": run.get("batch_id"),
        "status": run["status"],
        "stage": run["stage"],
        "stage_index": run["stage_index"],
        "stage_total": len(stages),
        "stages": stages,
        "rows": run["rows"],
        "sampled_rows": run.get("sampled_rows"),
        "pending_slots": run.get("pending_slots"),
        "total_score": run["total_score"],
        "grade": run["grade"],
        "error": run.get("error"),
        "alerts": (report.get("alerts") or {}).get("total"),
        "elapsed_ms": report.get("elapsed_ms"),
        "sampling_status": run.get("sampling_status"),
        "reconcile_status": run.get("reconcile_status"),
        "publish_status": run.get("publish_status"),
        "sampling_counts": counts,
        "reconcile_blockers": reconcile_result.get("blockers") or [],
        "result_url": f"/result/{run_id}",
    }


@app.get("/api/runs/{run_id}/alerts.csv", response_class=PlainTextResponse)
def alerts_csv(run_id: str):
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {run_id} 不存在。")
    if run["status"] != "done" or not run.get("report"):
        raise HTTPException(status_code=409, detail="该次运行尚未完成，暂不能导出工单。")
    if run.get("publish_status") != "published":
        raise HTTPException(status_code=409, detail="抽样清单与源数据对账未齐套，工单暂不发布。")
    csv_text = export.alerts_to_csv(run["report"]["alerts"])
    filename = f"qualiscan-workorder-{run_id}.csv"
    return PlainTextResponse(
        csv_text,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/rules/weights")
def get_weights() -> dict:
    r = rules()
    overrides = store.weight_overrides()
    columns, rule_weights = score.merged_weights(r, overrides)
    return {
        "columns": columns,
        "rule_weights": rule_weights,
        "defaults": {
            "columns": {n: float(s.get("weight", 1.0)) for n, s in r["columns"].items()},
            "rule_weights": {k: float(v) for k, v in r.get("rule_weights", {}).items()},
        },
        "overrides": overrides,
    }


@app.post("/api/rules/weights")
def set_weights(payload: dict) -> dict:
    r = rules()
    payload = payload or {}
    raw_columns = payload.get("columns") or {}
    raw_rules = payload.get("rule_weights") or {}
    columns: dict[str, float] = {}
    rule_weights: dict[str, float] = {}
    for name, value in raw_columns.items():
        if name not in r["columns"]:
            raise HTTPException(status_code=400, detail=f"未知列 {name}，无法设置权重。")
        columns[name] = _parse_weight(value, f"列 {name} 权重")
    for name, value in raw_rules.items():
        if name not in r.get("rule_weights", {}):
            raise HTTPException(status_code=400, detail=f"未知规则 {name}，无法设置权重。")
        rule_weights[name] = _parse_weight(value, f"规则 {name} 权重")
    saved = store.save_weight_overrides(columns, rule_weights)
    merged_columns, merged_rules = score.merged_weights(r, saved)
    return {"ok": True, "columns": merged_columns, "rule_weights": merged_rules}


def _parse_weight(value, label: str) -> float:
    try:
        weight = float(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"{label} 必须是数字。") from exc
    if weight < 0 or weight > 10:
        raise HTTPException(status_code=400, detail=f"{label} 需在 0 ~ 10 之间。")
    return round(weight, 3)


@app.post("/api/rules/weights/reset")
def reset_weights() -> dict:
    store.save_weight_overrides({}, {})
    return {"ok": True}


@app.post("/api/runs/{run_id}/rescore")
def rescore(run_id: str) -> dict:
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {run_id} 不存在。")
    if run["status"] != "done" or not run.get("report"):
        raise HTTPException(status_code=409, detail="该次运行尚未完成，无法重算。")
    if run.get("publish_status") != "published":
        raise HTTPException(status_code=409, detail="抽样清单与源数据对账未齐套，暂不能重算并发布。")
    r = rules()
    overrides = store.weight_overrides()
    report = run["report"]
    units = score.build_units(report["missing"], report["duplicates"],
                              report["outliers"], report["validation"], r)
    report["score"] = score.compute(units, r, overrides)
    store.update_report(run_id, report, overrides)
    return {
        "run_id": run_id,
        "total": report["score"]["total"],
        "grade": report["score"]["grade"],
        "weights": report["score"]["weights_used"],
    }


# ---------------------------------------------------------------- 抽样 / 撤回 / 重规划 / 对账 / 续跑


def _require_run(run_id: str) -> dict:
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {run_id} 不存在。")
    return run


def _require_sampling(run: dict) -> dict:
    sampling = run.get("sampling")
    if not sampling:
        raise HTTPException(status_code=409, detail="该次运行尚无抽样清单，请先执行抽样。")
    return sampling


@app.get("/api/runs/{run_id}/sampling")
def get_sampling(run_id: str, status: str | None = None, limit: int = 500,
                 offset: int = 0) -> dict:
    run = _require_run(run_id)
    sampling = _require_sampling(run)
    entries = sampling["entries"]
    if status:
        wanted = {s.strip() for s in status.split(",") if s.strip()}
        entries = [e for e in entries if e["status"] in wanted]
    limit = max(1, min(2000, limit))
    offset = max(0, offset)
    return {
        "run_id": run_id,
        "batch_id": sampling.get("batch_id"),
        "seed": sampling.get("seed"),
        "plan": sampling.get("plan"),
        "fingerprint": sampling.get("fingerprint"),
        "counts": sampling.get("counts"),
        "categories": sampling.get("categories"),
        "total_entries": len(sampling["entries"]),
        "offset": offset,
        "limit": limit,
        "entries": entries[offset:offset + limit],
        "reconcile": run.get("reconcile"),
        "publish_status": run.get("publish_status"),
    }


@app.get("/api/runs/{run_id}/sampling.csv", response_class=PlainTextResponse)
def sampling_csv(run_id: str, status: str | None = None):
    run = _require_run(run_id)
    sampling = _require_sampling(run)
    entries = sampling["entries"]
    if status:
        wanted = {s.strip() for s in status.split(",") if s.strip()}
        entries = [e for e in entries if e["status"] in wanted]
    csv_text = export.sampling_to_csv(sampling, entries)
    filename = f"qualiscan-sampling-{run_id}.csv"
    return PlainTextResponse(
        csv_text,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/runs/{run_id}/sampling/replay")
def replay_sampling(run_id: str) -> dict:
    run = _require_run(run_id)
    sampling = _require_sampling(run)
    batch = store.batch_rows([sampling["batch_id"]]).get(sampling["batch_id"])
    if not batch:
        raise HTTPException(status_code=409, detail="批次源数据快照缺失，无法重放。")
    result = sampling_mod.replay(sampling, batch["rows"], batch["columns"])
    return {"run_id": run_id, **result}


@app.post("/api/runs/{run_id}/sampling/withdraw")
def withdraw_row(run_id: str, payload: dict) -> dict:
    run = _require_run(run_id)
    sampling = _require_sampling(run)
    payload = payload or {}
    row_id = str(payload.get("row_id") or "").strip()
    if not row_id:
        raise HTTPException(status_code=400, detail="必须指定要撤回的 row_id。")
    actor = str(payload.get("actor") or "").strip()
    if not actor:
        raise HTTPException(status_code=400, detail="撤回必须记录操作人 actor。")
    reason = str(payload.get("reason") or "").strip()
    if not reason:
        raise HTTPException(status_code=400, detail="撤回必须填写原因 reason，便于审计追溯。")
    supply = store.batch_supply(run_id, limit_runs=10,
                                exclude_batch_id=(run.get("batch_id") or run_id))
    try:
        result = sampling_mod.withdraw(sampling, row_id, actor, reason, store.now(), supply)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    events = store.add_audit_events(run_id, result["events"])
    store.save_sampling(run_id, result["sampling"], run.get("reconcile"))
    _reconcile_after_change(run_id, result["sampling"])
    return {
        "run_id": run_id,
        "row_id": row_id,
        "events": events,
        "counts": result["sampling"]["counts"],
        "categories": result["sampling"]["categories"],
        "publish_status": store.get_run(run_id).get("publish_status"),
    }


@app.post("/api/runs/{run_id}/sampling/replan")
def replan_sampling(run_id: str, payload: dict) -> dict:
    run = _require_run(run_id)
    sampling = _require_sampling(run)
    payload = payload or {}
    quotas = _parse_quotas(payload.get("category_quota"))
    if not quotas:
        raise HTTPException(status_code=400, detail="必须提供新的 category_quota（品类: 配额）。")
    actor = str(payload.get("actor") or "复核人").strip()
    supply = store.batch_supply(run_id, limit_runs=10,
                                exclude_batch_id=(run.get("batch_id") or run_id))
    result = sampling_mod.replan(sampling, quotas, store.now(), actor, supply)
    events = store.add_audit_events(run_id, result["events"])
    store.save_sampling(run_id, result["sampling"], run.get("reconcile"))
    _reconcile_after_change(run_id, result["sampling"])
    return {
        "run_id": run_id,
        "quotas": quotas,
        "events": events,
        "counts": result["sampling"]["counts"],
        "categories": result["sampling"]["categories"],
        "publish_status": store.get_run(run_id).get("publish_status"),
    }


def _reconcile_after_change(run_id: str, sampling: dict) -> dict:
    """清单变更后立即重新对账并同步发布状态；不齐套则撤回发布。"""
    batch = store.batch_rows([sampling["batch_id"]]).get(sampling["batch_id"])
    if not batch:
        store.set_run_state(run_id, "blocked", "对账失败：批次快照缺失", 2,
                            error="批次源数据快照缺失，无法对账")
        store.set_publish_status(run_id, "withheld")
        return {}
    external = {
        bid: info for bid, info in store.batch_rows(
            sorted({e["source_batch"] for e in sampling["entries"] if e.get("source_batch")})
        ).items() if bid != sampling["batch_id"]
    }
    events = store.list_audit_events(run_id)
    reconcile_result = sampling_mod.reconcile(
        sampling, sampling["batch_id"], batch["rows"], batch["columns"],
        sampling["fingerprint"], external_batches=external, events=events, at=store.now(),
    )
    store.save_sampling(run_id, sampling, reconcile_result)
    if reconcile_result["status"] != "complete":
        store.set_publish_status(run_id, "withheld")
        store.set_run_state(run_id, "blocked", "对账未齐套，暂停发布", 2,
                            error="抽样清单与源数据对账未齐套：" + "；".join(
                                reconcile_result["blockers"][:3]))
    else:
        store.set_publish_status(run_id, "republish_required")
        store.set_run_state(run_id, "blocked", "清单已变更，待重新计算并发布", 3)
    return reconcile_result


@app.get("/api/runs/{run_id}/audit")
def audit_trail(run_id: str) -> dict:
    _require_run(run_id)
    events = store.list_audit_events(run_id)
    return {"run_id": run_id, "total": len(events), "events": events}


@app.post("/api/runs/{run_id}/resume")
def resume_run(run_id: str) -> dict:
    """从已确认阶段续跑；未完成阶段不会进入结果。"""
    run = _require_run(run_id)
    if run["status"] == "running":
        raise HTTPException(status_code=409, detail="该次运行仍在执行中，无需续跑。")
    if run.get("publish_status") == "published":
        raise HTTPException(status_code=409, detail="该次运行已发布，无需续跑。")
    if not run.get("checkpoint"):
        raise HTTPException(status_code=409, detail="没有可续跑的断点记录，请重新发起抽样。")
    store.set_run_state(run_id, "running", "从断点续跑", 0)

    def worker() -> None:
        try:
            orchestrate.execute(run_id, resume=True)
        except Exception as exc:  # noqa: BLE001
            store.fail_run(run_id, f"{type(exc).__name__}: {exc}")

    threading.Thread(target=worker, name=f"resume-{run_id}", daemon=True).start()
    return {"run_id": run_id, "status": "running", "resumed": True}


@app.post("/api/runs/{run_id}/publish")
def publish_run(run_id: str) -> dict:
    """重新执行对账；齐套后重算并发布，未齐套保持 withheld。"""
    run = _require_run(run_id)
    if run["status"] == "running":
        raise HTTPException(status_code=409, detail="该次运行仍在执行中，不能重复发布。")
    sampling = _require_sampling(run)
    reconcile_result = _reconcile_after_change(run_id, sampling)
    if not reconcile_result.get("status") == "complete":
        raise HTTPException(status_code=409, detail="抽样清单与源数据对账未齐套，暂不能发布：" +
                            "；".join((reconcile_result.get("blockers") or [])[:3]))
    store.set_run_state(run_id, "running", "发布前重算", 3)

    def worker() -> None:
        try:
            orchestrate.recompute(run_id)
        except Exception as exc:  # noqa: BLE001
            store.fail_run(run_id, f"{type(exc).__name__}: {exc}")

    threading.Thread(target=worker, name=f"publish-{run_id}", daemon=True).start()
    return {"run_id": run_id, "status": "running", "reconcile": reconcile_result}


@app.get("/api/runs/{run_id}/reconcile")
def get_reconcile(run_id: str) -> dict:
    run = _require_run(run_id)
    return {
        "run_id": run_id,
        "sampling_status": run.get("sampling_status"),
        "reconcile_status": run.get("reconcile_status"),
        "publish_status": run.get("publish_status"),
        "reconcile": run.get("reconcile"),
    }


@app.get("/health")
def health() -> JSONResponse:
    ensure_seeded()
    return JSONResponse({
        "status": "ok",
        "app": settings.APP_SHORT,
        "runs": store.count_runs(),
        "rules_version": rules().get("version"),
        "data_dir": str(settings.DATA_DIR),
    })


@app.get("/api/rules")
def rules_api() -> dict:
    return rules()


@app.get("/api/runs")
def runs_api(limit: int = 20) -> dict:
    return {"runs": store.list_runs(limit=max(1, min(100, limit)))}
