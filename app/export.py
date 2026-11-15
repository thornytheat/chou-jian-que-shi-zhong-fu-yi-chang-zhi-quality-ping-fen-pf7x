"""工单导出：把去重后的告警渲染成可直接下发的 CSV（Excel 友好，带 BOM）。"""
from __future__ import annotations

import csv
import io

HEADER = ["工单号", "row_id", "column", "rules", "severity", "当前值", "问题说明", "整改建议"]

SAMPLING_HEADER = [
    "row_id", "品类", "状态", "配额", "替补顺位", "本批/补录顺位", "来源批次",
    "行哈希", "去向说明",
]
SAMPLING_STATUS_LABELS = {
    "selected": "入选", "replacement": "替补", "candidate": "候选",
    "withdrawn": "撤回", "pending": "未决", "invalid": "无效",
}


def alerts_to_csv(alerts: dict) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(HEADER)
    for index, item in enumerate(alerts["items"], start=1):
        writer.writerow([
            f"WO-{index:04d}",
            item["row_id"],
            item["column"],
            item["rules_text"],
            item["severity"],
            item["value"],
            item["reason"],
            item["suggestion"],
        ])
    return "\ufeff" + buffer.getvalue()


def sampling_to_csv(sampling: dict, entries: list[dict]) -> str:
    """抽样清单：逐行写清候选/入选/替补/未决/撤回状态与替补去向，用于复核与重放留档。"""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(SAMPLING_HEADER)
    for entry in entries:
        is_pending = str(entry.get("row_id", "")).startswith("__pending__:")
        writer.writerow([
            entry["row_id"],
            entry.get("category") or "—",
            SAMPLING_STATUS_LABELS.get(entry["status"], entry["status"]),
            entry.get("quota"),
            entry.get("slot") if entry.get("status") == "replacement" else "",
            entry.get("rank_index") if entry.get("rank_index") is not None else "",
            entry.get("source_batch") or ("待补录" if is_pending else "本批"),
            (entry.get("row_hash") or "")[:16],
            entry.get("disposition") or "",
        ])
    return "﻿" + buffer.getvalue()
