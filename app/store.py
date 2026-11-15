"""SQLite 访问：运行记录（含报告快照）与规则权重覆盖。

只有一个表 runs 和一个键值表 settings，够用即可，不引入 ORM。
报告整份以 JSON 存在 runs.report_json，重算权重时无需重新解析全部单元格。
"""
from __future__ import annotations

import datetime
import json
import sqlite3
import threading

from . import settings

_LOCK = threading.Lock()
_CONN: sqlite3.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  source TEXT NOT NULL,
  status TEXT NOT NULL,
  stage TEXT NOT NULL,
  stage_index INTEGER NOT NULL DEFAULT 0,
  rows INTEGER NOT NULL DEFAULT 0,
  total_score REAL,
  grade TEXT,
  weight_overrides TEXT,
  report_json TEXT,
  error TEXT,
  batch_id TEXT,
  input_path TEXT,
  quota_overrides TEXT,
  sampling_seed TEXT,
  sampled_rows INTEGER NOT NULL DEFAULT 0,
  candidate_rows INTEGER NOT NULL DEFAULT 0,
  replacement_rows INTEGER NOT NULL DEFAULT 0,
  withdrawn_rows INTEGER NOT NULL DEFAULT 0,
  pending_slots INTEGER NOT NULL DEFAULT 0,
  --- 抽样 / 对账 / 发布（新增列，见 _migrate）
  sampling_status TEXT NOT NULL DEFAULT 'pending',
  reconcile_status TEXT NOT NULL DEFAULT 'pending',
  publish_status TEXT NOT NULL DEFAULT 'unpublished',
  sampling_json TEXT,
  reconcile_json TEXT,
  checkpoint_json TEXT
);
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  action TEXT NOT NULL,
  row_id TEXT,
  category TEXT,
  from_status TEXT,
  to_status TEXT,
  actor TEXT,
  reason TEXT,
  at TEXT NOT NULL,
  note TEXT
);
CREATE TABLE IF NOT EXISTS run_batches (
  batch_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  columns_json TEXT NOT NULL,
  rows_json TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  created_at TEXT NOT NULL
);
"""

MIGRATION_COLUMNS = {
    "batch_id": "TEXT",
    "input_path": "TEXT",
    "quota_overrides": "TEXT",
    "sampling_seed": "TEXT",
    "sampling_status": "TEXT NOT NULL DEFAULT 'pending'",
    "reconcile_status": "TEXT NOT NULL DEFAULT 'pending'",
    "publish_status": "TEXT NOT NULL DEFAULT 'unpublished'",
    "sampling_json": "TEXT",
    "reconcile_json": "TEXT",
    "checkpoint_json": "TEXT",
    "sampled_rows": "INTEGER NOT NULL DEFAULT 0",
    "candidate_rows": "INTEGER NOT NULL DEFAULT 0",
    "replacement_rows": "INTEGER NOT NULL DEFAULT 0",
    "withdrawn_rows": "INTEGER NOT NULL DEFAULT 0",
    "pending_slots": "INTEGER NOT NULL DEFAULT 0",
}


def connect() -> sqlite3.Connection:
    global _CONN
    with _LOCK:
        if _CONN is None:
            settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
            _CONN = sqlite3.connect(settings.DB_PATH, check_same_thread=False)
            _CONN.row_factory = sqlite3.Row
            _CONN.execute("PRAGMA journal_mode=WAL")
            _CONN.executescript(SCHEMA)
            _migrate(_CONN)
            _CONN.commit()
        return _CONN


def _migrate(conn: sqlite3.Connection) -> None:
    """老库补列（SQLite 无 IF NOT EXISTS ADD COLUMN，逐个尝试）。"""
    existing = {row["name"] for row in conn.execute("PRAGMA table_info(runs)").fetchall()}
    for name, ddl in MIGRATION_COLUMNS.items():
        if name in existing:
            continue
        conn.execute(f"ALTER TABLE runs ADD COLUMN {name} {ddl}")


def reset_connection() -> None:
    """测试用：切换 DB 路径后重建连接。"""
    global _CONN
    with _LOCK:
        if _CONN is not None:
            _CONN.close()
        _CONN = None


def now() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def create_run(run_id: str, source: str, weight_overrides: dict | None,
               batch_id: str | None = None, input_path: str | None = None,
               quota_overrides: dict | None = None, sampling_seed: str | None = None) -> None:
    conn = connect()
    with _LOCK:
        conn.execute(
            "INSERT INTO runs (run_id, created_at, source, status, stage, stage_index,"
            " weight_overrides, batch_id, input_path, quota_overrides, sampling_seed)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, now(), source, "running", "排队中", 0,
             json.dumps(weight_overrides or {}, ensure_ascii=False),
             batch_id or run_id, input_path,
             json.dumps(quota_overrides or {}, ensure_ascii=False),
             sampling_seed),
        )
        conn.commit()


def set_progress(run_id: str, stage_index: int, stage: str) -> None:
    conn = connect()
    with _LOCK:
        conn.execute(
            "UPDATE runs SET stage_index=?, stage=? WHERE run_id=?",
            (stage_index, stage, run_id),
        )
        conn.commit()


def set_run_state(run_id: str, status: str, stage: str, stage_index: int,
                  error: str | None = None) -> None:
    """更新运行状态（running / blocked / failed / done），不动报告与抽样快照。"""
    conn = connect()
    with _LOCK:
        if error is None:
            conn.execute(
                "UPDATE runs SET status=?, stage=?, stage_index=? WHERE run_id=?",
                (status, stage, stage_index, run_id),
            )
        else:
            conn.execute(
                "UPDATE runs SET status=?, stage=?, stage_index=?, error=? WHERE run_id=?",
                (status, stage, stage_index, error, run_id),
            )
        conn.commit()


def begin_execution(run_id: str) -> None:
    """开始一次（重）计算前清空上一轮已发布结果，确保未完成阶段绝不进入结果。

    抽样清单、对账证据与断点保留；只清 report/score/grade 与发布状态，
    完成时再由 finish_run 原子写回。
    """
    conn = connect()
    with _LOCK:
        conn.execute(
            "UPDATE runs SET status='running', error=NULL, report_json=NULL,"
            " total_score=NULL, grade=NULL, publish_status='withheld' WHERE run_id=?",
            (run_id,),
        )
        conn.commit()


def finish_run(run_id: str, report: dict, *, sampling: dict | None = None,
               reconcile_result: dict | None = None) -> None:
    conn = connect()
    score = report["score"]
    counts = (sampling or {}).get("counts") or {}
    withheld = (reconcile_result or {}).get("status", "skipped") != "complete"
    with _LOCK:
        conn.execute(
            "UPDATE runs SET status=?, stage=?, stage_index=99, rows=?,"
            " total_score=?, grade=?, report_json=?,"
            " sampling_json=?, reconcile_json=?, sampled_rows=?, candidate_rows=?,"
            " replacement_rows=?, withdrawn_rows=?, pending_slots=?,"
            " sampling_status=?, reconcile_status=?, publish_status=?"
            " WHERE run_id=?",
            ("blocked" if withheld else "done",
             "对账未齐套，结果暂不发布" if withheld else "已发布",
             report["dataset"]["rows"], score["total"], score["grade"],
             json.dumps(report, ensure_ascii=False),
             json.dumps(sampling, ensure_ascii=False),
             json.dumps(reconcile_result, ensure_ascii=False),
             int(counts.get("selected") or 0),
             int(counts.get("candidate_backlog") or 0),
             int(counts.get("replacement") or 0),
             int(counts.get("withdrawn") or 0),
             int(counts.get("pending_slots") or 0),
             "ready" if sampling else "skipped",
             (reconcile_result or {}).get("status", "skipped"),
             "withheld" if withheld else "published",
             run_id),
        )
        conn.commit()


def fail_run(run_id: str, message: str) -> None:
    conn = connect()
    with _LOCK:
        conn.execute(
            "UPDATE runs SET status='failed', stage='失败', error=?,"
            " publish_status='withheld' WHERE run_id=?",
            (message, run_id),
        )
        conn.commit()


def save_checkpoint(run_id: str, checkpoint: dict) -> None:
    conn = connect()
    with _LOCK:
        conn.execute(
            "UPDATE runs SET checkpoint_json=? WHERE run_id=?",
            (json.dumps(checkpoint, ensure_ascii=False), run_id),
        )
        conn.commit()


def save_sampling(run_id: str, sampling: dict, reconcile_result: dict | None) -> None:
    conn = connect()
    counts = sampling.get("counts") or {}
    with _LOCK:
        conn.execute(
            "UPDATE runs SET sampling_json=?, reconcile_json=?, sampled_rows=?, candidate_rows=?,"
            " replacement_rows=?, withdrawn_rows=?, pending_slots=?,"
            " sampling_status='ready', reconcile_status=?,"
            " publish_status=CASE WHEN ?='complete' THEN publish_status ELSE 'withheld' END"
            " WHERE run_id=?",
            (json.dumps(sampling, ensure_ascii=False),
             json.dumps(reconcile_result, ensure_ascii=False) if reconcile_result else None,
             int(counts.get("selected") or 0),
             int(counts.get("candidate_backlog") or 0),
             int(counts.get("replacement") or 0),
             int(counts.get("withdrawn") or 0),
             int(counts.get("pending_slots") or 0),
             (reconcile_result or {}).get("status", "pending"),
             (reconcile_result or {}).get("status", "pending"),
             run_id),
        )
        conn.commit()


def set_publish_status(run_id: str, status: str) -> None:
    conn = connect()
    with _LOCK:
        conn.execute("UPDATE runs SET publish_status=? WHERE run_id=?", (status, run_id))
        conn.commit()


def add_audit_events(run_id: str, events: list[dict]) -> list[dict]:
    """写入审计事件并返回带 seq 的完整记录。只增不改，撤回痕迹永久保留。"""
    conn = connect()
    stored: list[dict] = []
    with _LOCK:
        for event in events:
            cursor = conn.execute(
                "INSERT INTO audit_events (run_id, action, row_id, category, from_status,"
                " to_status, actor, reason, at, note) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (run_id, event.get("action", ""), event.get("row_id"), event.get("category"),
                 event.get("from_status"), event.get("to_status"), event.get("actor"),
                 event.get("reason"), event.get("at") or now(), event.get("note")),
            )
            stored.append({**event, "seq": cursor.lastrowid, "run_id": run_id})
        conn.commit()
    return stored


def list_audit_events(run_id: str, limit: int = 500) -> list[dict]:
    conn = connect()
    rows = conn.execute(
        "SELECT * FROM audit_events WHERE run_id=? ORDER BY seq ASC LIMIT ?",
        (run_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def batch_supply(exclude_run_id: str, category: str | None = None,
                 limit_runs: int = 10, exclude_batch_id: str | None = None) -> list[dict]:
    """既往批次候选池：从已完成的运行里取候选/替补行，供跨批补录。

    同一批次号（确定性派生或显式标签）的旧运行会被排除：跨批补录只能来自**其他批次**。
    """
    conn = connect()
    rows = conn.execute(
        "SELECT run_id, batch_id, sampling_json FROM runs"
        " WHERE status='done' AND sampling_json IS NOT NULL AND run_id<>?"
        " ORDER BY created_at DESC, run_id DESC LIMIT ?",
        (exclude_run_id, limit_runs),
    ).fetchall()
    supply: list[dict] = []
    for row in rows:
        sampling = json.loads(row["sampling_json"])
        batch_id = sampling.get("batch_id") or row["batch_id"] or row["run_id"]
        if exclude_batch_id and batch_id == exclude_batch_id:
            continue
        for entry in sampling.get("entries", []):
            if entry.get("status") not in ("candidate", "replacement"):
                continue
            if category and entry.get("category") != category:
                continue
            supply.append({
                "row_id": entry["row_id"],
                "category": entry.get("category"),
                "source_batch": batch_id,
                "source_run": row["run_id"],
                "rank_hash": entry.get("rank_hash"),
                "rank_index": entry.get("rank_index"),
                "row": entry.get("row") or {"row_id": entry["row_id"],
                                            "category": entry.get("category")},
            })
    return supply


def batch_rows(batch_ids: list[str]) -> dict[str, dict]:
    """取来源批次的源数据快照（对账跨批来源漂移用）。"""
    if not batch_ids:
        return {}
    conn = connect()
    placeholders = ",".join("?" for _ in batch_ids)
    rows = conn.execute(
        f"SELECT batch_id, columns_json, rows_json, fingerprint FROM run_batches"
        f" WHERE batch_id IN ({placeholders})",
        tuple(batch_ids),
    ).fetchall()
    return {
        row["batch_id"]: {
            "rows": json.loads(row["rows_json"]),
            "columns": json.loads(row["columns_json"]),
            "fingerprint": row["fingerprint"],
        }
        for row in rows
    }


def save_batch(batch_id: str, rows: list[dict], columns: list[str], fp: str,
               kind: str = "sample") -> bool:
    """批次源快照：跨批补录与对账需要回到来源批次核对，必须可重放。

    返回是否新建；同批次号已存在且指纹不同 → 返回 False（调用方应报错，避免静默换数据）。
    """
    conn = connect()
    with _LOCK:
        existing = conn.execute(
            "SELECT fingerprint FROM run_batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if existing is not None:
            return existing["fingerprint"] == fp
        conn.execute(
            "INSERT INTO run_batches (batch_id, kind, columns_json, rows_json, fingerprint, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (batch_id, kind, json.dumps(columns, ensure_ascii=False),
             json.dumps(rows, ensure_ascii=False), fp, now()),
        )
        conn.commit()
        return True


def get_run(run_id: str) -> dict | None:
    conn = connect()
    row = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def list_runs(limit: int = 20) -> list[dict]:
    conn = connect()
    rows = conn.execute(
        "SELECT run_id, created_at, source, status, stage, rows, total_score, grade, error,"
        " batch_id, sampled_rows, candidate_rows, replacement_rows, withdrawn_rows,"
        " pending_slots, sampling_status, reconcile_status, publish_status"
        " FROM runs ORDER BY created_at DESC, run_id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def update_report(run_id: str, report: dict, weights: dict | None) -> None:
    conn = connect()
    score = report["score"]
    with _LOCK:
        conn.execute(
            "UPDATE runs SET total_score=?, grade=?, report_json=?, weight_overrides=? WHERE run_id=?",
            (score["total"], score["grade"], json.dumps(report, ensure_ascii=False),
             json.dumps(weights or {}, ensure_ascii=False), run_id),
        )
        conn.commit()


def count_runs() -> int:
    conn = connect()
    row = conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()
    return int(row["n"])


def _row_to_dict(row: sqlite3.Row) -> dict:
    data = dict(row)
    for field, default in (("report", "report_json"), ("sampling", "sampling_json"),
                           ("reconcile", "reconcile_json"), ("checkpoint", "checkpoint_json")):
        raw = data.pop(default, None)
        try:
            data[field] = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            data[field] = None
    for field in ("weight_overrides", "quota_overrides"):
        try:
            data[field] = json.loads(data.get(field) or "{}")
        except json.JSONDecodeError:
            data[field] = {}
    return data


def get_setting(key: str, default: str | None = None) -> str | None:
    conn = connect()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str) -> None:
    conn = connect()
    with _LOCK:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?,?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()


def weight_overrides() -> dict:
    raw = get_setting("weight_overrides", "{}") or "{}"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return {
        "columns": {k: float(v) for k, v in (data.get("columns") or {}).items()},
        "rule_weights": {k: float(v) for k, v in (data.get("rule_weights") or {}).items()},
    }


def save_weight_overrides(columns: dict, rule_weights: dict) -> dict:
    payload = {
        "columns": {k: float(v) for k, v in columns.items()},
        "rule_weights": {k: float(v) for k, v in rule_weights.items()},
    }
    set_setting("weight_overrides", json.dumps(payload, ensure_ascii=False))
    return payload
