"""按品类配额生成可重放的抽样清单，并负责与源数据对账。

核心口径
--------
- 可重放：入选只由 ``sha256(seed|batch_id|row_id)`` 的升序决定，不依赖行序、不依赖
  随机数状态；同一批次 + 同一计划 + 同一候选池永远得到同一份清单。
- 配额：每个品类从本批候选里按哈希顺位取前 N（N 来自规则集 ``sampling.category_quota``，
  未列出的品类取 ``default_quota``）。
- 替补：配额之后的前 ``replacement_pool`` 行进入替补池，写明「替补池第 i 顺位」；
  其余候选保留为 ``candidate``（候选未入选）。
- 候选不足：本批候选不足时按计划从**既往批次**的候选池跨批补录，补录行写明来源批次；
  仍不足则生成 ``pending`` 占位（未决），在对账齐套前不得发布。
- 超配：重规划（``replan``）把配额下调后，超出新配额的入选行按顺位转入替补池（或退回
  来源批次候选池），每一行去向都写在 ``disposition`` 里。
- 撤回：状态转 ``withdrawn``，保留撤回人/原因/时间；同品类替补池顺位最高者顶上，
  替补池为空则尝试跨批补录，仍无则记录未决。
- 对账：逐行校验 row_id 存在、品类一致、行哈希一致、跨批来源可定位且未漂移、品类配额
  配齐、撤回有审计事件；全部通过才是 ``complete``（齐套），才允许发布分数/异常组/工单。
"""
from __future__ import annotations

import hashlib
import json
import math

SELECTED = "selected"
REPLACEMENT = "replacement"
CANDIDATE = "candidate"
WITHDRAWN = "withdrawn"
PENDING = "pending"
INVALID = "invalid"

STATUS_LABELS = {
    SELECTED: "入选",
    REPLACEMENT: "替补",
    CANDIDATE: "候选",
    WITHDRAWN: "撤回",
    PENDING: "未决",
    INVALID: "无效",
}

PENDING_ROW_PREFIX = "__pending__:"

DEFAULT_PLAN = {
    "seed": "qualiscan-default",
    "default_quota": 10,
    "category_quota": {},
    "replacement_pool": 2,
    "cross_batch": {"enabled": True, "max_per_category": 100},
    "over_quota_allowance": 0.0,
}


# ---------------------------------------------------------------- 哈希与计划

def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def row_hash(row: dict, columns: list[str]) -> str:
    """单行指纹：只依赖列名与取值，与行序无关。"""
    payload = json.dumps(
        [[c, str(row.get(c, ""))] for c in columns],
        ensure_ascii=False, separators=(",", ":"),
    )
    return _sha(payload)


def fingerprint(rows: list[dict], columns: list[str]) -> str:
    """源数据指纹：对全部行哈希排序后再哈希，换序不改变指纹。"""
    return _sha("\n".join(sorted(row_hash(r, columns) for r in rows)))


def rank_key(seed: str, batch_id: str, row_id: str) -> str:
    """抽样顺位：只由 seed + 批次 + row_id 决定，可重放。"""
    return _sha(f"{seed}|{batch_id}|{row_id}")


def derive_batch_id(seed: str, source: str, fp: str, label: str | None = None) -> str:
    """默认批次号由 种子 + 数据源 + 源指纹 派生：同一输入再跑一次得到同一清单。

    月度多批次需要区分时，显式传 label（如 ``2026-11``）即可得到独立批次。
    """
    if label:
        return f"{label}"
    return "batch-" + _sha(f"{seed}|{source}|{fp}")[:16]


def plan_from_rules(rules: dict, quota_overrides: dict | None = None,
                    seed: str | None = None) -> dict:
    spec = {**DEFAULT_PLAN, **(rules.get("sampling") or {})}
    cross = {**DEFAULT_PLAN["cross_batch"], **(spec.get("cross_batch") or {})}
    quotas = {str(k): _quota(v) for k, v in (spec.get("category_quota") or {}).items()}
    for name, value in (quota_overrides or {}).items():
        quotas[str(name)] = _quota(value)
    return {
        "seed": str(seed or spec.get("seed") or DEFAULT_PLAN["seed"]),
        "default_quota": _quota(spec.get("default_quota", DEFAULT_PLAN["default_quota"])),
        "category_quota": dict(sorted(quotas.items())),
        "replacement_pool": max(0, int(spec.get("replacement_pool", 2) or 0)),
        "cross_batch": {
            "enabled": bool(cross.get("enabled", True)),
            "max_per_category": max(0, int(cross.get("max_per_category", 100) or 0)),
        },
        "over_quota_allowance": _rate(spec.get("over_quota_allowance", 0.0)),
    }


def quota_for(plan: dict, category: str) -> int:
    return int(plan["category_quota"].get(category, plan["default_quota"]))


def allowed_over(plan: dict, category: str) -> int:
    """允许保留的超配行数（配额 × 容忍比例，向下取整）。"""
    return int(math.floor(quota_for(plan, category) * plan.get("over_quota_allowance", 0.0)))


def _quota(value) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _rate(value) -> float:
    try:
        return min(1.0, max(0.0, float(value)))
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------- 抽样

def select(rows: list[dict], columns: list[str], plan: dict, batch_id: str,
           supply: list[dict] | None = None) -> dict:
    """生成抽样清单。

    supply 为既往批次候选池快照，形如
    ``{"row_id","category","source_batch","rank_hash","rank_index","row"}``。
    """
    supply = supply or []
    seed = plan["seed"]
    pool_size = plan["replacement_pool"]
    cross = plan["cross_batch"]

    by_category: dict[str, list[dict]] = {}
    invalid: list[dict] = []
    for row in rows:
        category = str(row.get("category", "") or "").strip()
        if not category:
            invalid.append(row)
            continue
        by_category.setdefault(category, []).append(row)

    supply_by_category: dict[str, list[dict]] = {}
    for item in supply:
        supply_by_category.setdefault(str(item["category"]), []).append(item)

    # 只对本批实际出现的品类生成配额；规则集里配置但本批没有的品类不产生未决
    categories = sorted(by_category)
    entries: list[dict] = []
    used_row_ids: set[str] = set()

    for category in categories:
        quota = quota_for(plan, category)
        local = sorted(
            by_category.get(category, []),
            key=lambda r: (rank_key(seed, batch_id, r["row_id"]), r["row_id"]),
        )
        selected_local = local[:quota]
        pool_local = local[quota:quota + pool_size]
        backlog_local = local[quota + pool_size:]

        for index, row in enumerate(selected_local, start=1):
            entries.append(_local_entry(
                row, columns, category, SELECTED, seed, batch_id,
                rank_index=index, quota=quota, slot=index,
                disposition=f"配额内入选：{category} 配额 {quota}，本批哈希顺位 {index}/{len(local)}",
            ))
            used_row_ids.add(row["row_id"])

        shortfall = quota - len(selected_local)
        supplements: list[dict] = []
        if shortfall > 0 and cross["enabled"]:
            pool = [s for s in supply_by_category.get(category, [])
                    if s["row_id"] not in used_row_ids]
            pool.sort(key=lambda s: (s.get("rank_hash") or rank_key(seed, s["source_batch"], s["row_id"]),
                                     s["source_batch"], s["row_id"]))
            supplements = pool[:min(shortfall, cross["max_per_category"])]
        for offset, item in enumerate(supplements, start=1):
            row = item.get("row") or {"row_id": item["row_id"], "category": category}
            entries.append(_entry(
                row, columns, category, SELECTED,
                rank_hash=item.get("rank_hash") or rank_key(seed, item["source_batch"], item["row_id"]),
                row_hash_value=row_hash(row, columns),
                quota=quota, slot=quota + offset, rank_index=quota + offset,
                origin="cross_batch", source_batch=item["source_batch"],
                disposition=(
                    f"候选不足：本批候选 {len(local)} 行，按需跨批补录 {shortfall} 行；"
                    f"本行取自批次 {item['source_batch']} 的候选（原顺位 {item.get('rank_index') or '—'}），"
                    f"补入 {category} 第 {quota + offset} 个配额位"
                ),
            ))
            used_row_ids.add(item["row_id"])

        for offset, row in enumerate(pool_local, start=1):
            entries.append(_local_entry(
                row, columns, category, REPLACEMENT, seed, batch_id,
                rank_index=quota + offset, quota=quota, slot=offset,
                disposition=(
                    f"替补池第 {offset} 顺位：{category} 配额 {quota} 已满，"
                    f"本批哈希顺位 {quota + offset}/{len(local)}；入选行撤回时按顺位顶上"
                ),
            ))
        for offset, row in enumerate(backlog_local, start=quota + pool_size + 1):
            entries.append(_local_entry(
                row, columns, category, CANDIDATE, seed, batch_id,
                rank_index=offset, quota=quota, slot=None,
                disposition=(
                    f"候选未入选：{category} 配额 {quota} 与替补池（{pool_size} 行）已满，"
                    f"本批哈希顺位 {offset}/{len(local)}，保留为候选"
                ),
            ))

        remaining = shortfall - len(supplements)
        if remaining > 0:
            entries.append(_pending_entry(category, quota, remaining, seeds_seen=len(local),
                                          cross_in=len(supplements)))

    for row in invalid:
        entries.append(_local_entry(
            row, columns, "", INVALID, seed, batch_id,
            rank_index=None, quota=0, slot=None,
            disposition="品类缺失，无法按品类配额抽样（已由缺失规则单独命中）",
        ))

    sampling = {
        "batch_id": batch_id,
        "seed": seed,
        "columns": list(columns),
        "plan": plan,
        "fingerprint": fingerprint(rows, columns),
        "entries": entries,
        "operations": [],
        "replay_input": {
            "plan": plan,
            "batch_id": batch_id,
            "supply": [
                {"row_id": s["row_id"], "category": s["category"],
                 "source_batch": s["source_batch"], "rank_hash": s["rank_hash"],
                 "rank_index": s.get("rank_index"), "row": s.get("row")}
                for s in supply
            ],
        },
    }
    return recount(sampling)


def _local_entry(row, columns, category, status, seed, batch_id, *, rank_index, quota,
                 slot, disposition) -> dict:
    return _entry(
        row, columns, category, status,
        rank_hash=rank_key(seed, batch_id, row["row_id"]) if category else None,
        row_hash_value=row_hash(row, columns),
        quota=quota, slot=slot, rank_index=rank_index,
        origin="local", source_batch=None, disposition=disposition,
    )


def _entry(row, columns, category, status, *, rank_hash, row_hash_value, quota, slot,
           rank_index, origin, source_batch, disposition) -> dict:
    return {
        "row_id": row["row_id"],
        "category": category,
        "status": status,
        "disposition": disposition,
        "quota": quota,
        "rank_index": rank_index,
        "slot": slot,
        "rank_hash": rank_hash,
        "row_hash": row_hash_value,
        "source_batch": source_batch,
        "origin": origin,
        "row": {c: str(row.get(c, "")) for c in columns},
    }


def _pending_entry(category: str, quota: int, missing: int, *,
                   seeds_seen: int | None = None, cross_in: int = 0,
                   reason: str | None = None) -> dict:
    detail = reason or (
        f"候选不足：本批候选 {seeds_seen} 行、跨批补录 {cross_in} 行，仍缺 {missing} 行；"
        f"未决，待补录或调整配额后重算，未齐套不得发布"
    )
    return {
        "row_id": f"{PENDING_ROW_PREFIX}{category}",
        "category": category,
        "status": PENDING,
        "disposition": detail,
        "quota": quota,
        "rank_index": None,
        "slot": None,
        "rank_hash": None,
        "row_hash": None,
        "source_batch": None,
        "origin": "pending",
        "missing_slots": missing,
    }


# ---------------------------------------------------------------- 汇总

def recount(sampling: dict) -> dict:
    """按 entries 重算品类汇总与状态计数（抽样/撤回/重规划/补录共用）。"""
    entries = sampling["entries"]
    plan = sampling["plan"]
    batch_id = sampling["batch_id"]
    categories: dict[str, dict] = {}

    def report_for(category: str) -> dict:
        if category not in categories:
            quota = quota_for(plan, category)
            categories[category] = {
                "category": category,
                "quota": quota,
                "cap": quota + allowed_over(plan, category),
                "local_candidates": 0,
                "selected_local": 0,
                "cross_batch_in": 0,
                "replacement": 0,
                "candidate_backlog": 0,
                "withdrawn": 0,
                "unfilled": 0,
                "over_quota": 0,
            }
        return categories[category]

    statuses = {SELECTED: 0, REPLACEMENT: 0, CANDIDATE: 0, WITHDRAWN: 0, PENDING: 0, INVALID: 0}
    for entry in entries:
        category = entry.get("category") or ""
        status = entry["status"]
        statuses[status] = statuses.get(status, 0) + 1
        if not category:
            continue
        report = report_for(category)
        if entry.get("source_batch") in (None, batch_id) and status not in (INVALID, PENDING):
            report["local_candidates"] += 1
        if status == SELECTED:
            if entry.get("origin") == "cross_batch":
                report["cross_batch_in"] += 1
            else:
                report["selected_local"] += 1
            if entry.get("over_quota"):
                report["over_quota"] += 1
        elif status == REPLACEMENT:
            report["replacement"] += 1
        elif status == CANDIDATE:
            report["candidate_backlog"] += 1
        elif status == WITHDRAWN:
            report["withdrawn"] += 1
        elif status == PENDING:
            report["unfilled"] += int(entry.get("missing_slots") or 0)

    sampling["categories"] = [categories[c] for c in sorted(categories) if c]
    sampling["counts"] = {
        "status_counts": statuses,
        "selected": statuses[SELECTED],
        "replacement": statuses[REPLACEMENT],
        "candidate_backlog": statuses[CANDIDATE],
        "withdrawn": statuses[WITHDRAWN],
        "pending": statuses[PENDING],
        "pending_slots": sum(int(e.get("missing_slots") or 0) for e in entries
                             if e["status"] == PENDING),
        "invalid": statuses[INVALID],
        "cross_batch_in": sum(1 for e in entries if e.get("origin") == "cross_batch"
                              and e["status"] == SELECTED),
        "over_quota": sum(1 for e in entries if e.get("over_quota")),
        "candidate_supply": sum(1 for e in entries
                                if e.get("source_batch") in (None, batch_id)
                                and e["status"] in (SELECTED, REPLACEMENT, CANDIDATE, WITHDRAWN)),
    }
    return sampling


def _is_pending(entry: dict) -> bool:
    return str(entry.get("row_id", "")).startswith(PENDING_ROW_PREFIX)


def _selected_in(entries: list[dict], category: str) -> list[dict]:
    return [e for e in entries if e["category"] == category and e["status"] == SELECTED]


def _replacement_pool(entries: list[dict], category: str) -> list[dict]:
    return sorted(
        (e for e in entries if e["category"] == category and e["status"] == REPLACEMENT),
        key=lambda e: (e.get("slot") if e.get("slot") is not None else 10 ** 6, e["row_id"]),
    )


def _renumber_pool(entries: list[dict], category: str) -> None:
    for index, entry in enumerate(_replacement_pool(entries, category), start=1):
        entry["slot"] = index


# ---------------------------------------------------------------- 撤回

def withdraw(sampling: dict, row_id: str, actor: str, reason: str, at: str,
             supply: list[dict] | None = None) -> dict:
    """撤回入选/替补行：保留审计事件，顺位替补顶上；无替补则补录或记未决。

    返回 ``{"sampling": 新快照, "events": [...]}``。
    """
    entries = [dict(e) for e in sampling["entries"]]
    target = next((e for e in entries if e["row_id"] == row_id), None)
    if target is None:
        raise ValueError(f"批次 {sampling['batch_id']} 中不存在行 {row_id}")
    if target["status"] == WITHDRAWN:
        raise ValueError(f"行 {row_id} 已撤回（审计事件已保留），不能重复撤回")
    if target["status"] not in (SELECTED, REPLACEMENT):
        raise ValueError(
            f"行 {row_id} 当前为 {STATUS_LABELS.get(target['status'], target['status'])}，"
            "只有入选或替补行可以撤回"
        )

    from_status = target["status"]
    category = target["category"]
    target["status"] = WITHDRAWN
    target["withdrawn"] = {
        "from_status": from_status,
        "actor": actor,
        "reason": reason,
        "at": at,
        "disposition_before": target.get("disposition"),
    }
    target["disposition"] = (
        f"已撤回（原{STATUS_LABELS.get(from_status, from_status)}）：{reason}；"
        f"操作人 {actor}，{at}"
    )
    events = [{
        "action": "withdraw",
        "run_id": sampling["batch_id"],
        "row_id": row_id,
        "category": category,
        "from_status": from_status,
        "to_status": WITHDRAWN,
        "actor": actor,
        "reason": reason,
        "at": at,
        "note": target["disposition"],
    }]
    sampling.setdefault("operations", []).append({
        "type": "withdraw", "row_id": row_id, "actor": actor, "reason": reason, "at": at,
        "from_status": from_status,
    })

    if from_status == SELECTED:
        _fill_selection(entries, sampling, category, actor, at, supply or [], events,
                        trigger=f"撤回 {row_id}")
    else:
        _refill_pool(entries, category, actor, at, events, trigger=f"撤回替补 {row_id}")

    sampling["entries"] = entries
    return {"sampling": recount(sampling), "events": events}


def _fill_selection(entries, sampling, category, actor, at, supply, events, *, trigger):
    """入选位空缺：替补池顶上 → 跨批候选补录 → 未决。"""
    pool = _replacement_pool(entries, category)
    if pool:
        promoted = pool[0]
        promoted["status"] = SELECTED
        promoted["promoted_from"] = REPLACEMENT
        promoted["disposition"] = (
            f"替补入位：{trigger}；原为 {category} 替补池第 {promoted.get('slot') or 1} 顺位，现补入选"
        )
        events.append({
            "action": "promote", "run_id": sampling["batch_id"], "row_id": promoted["row_id"],
            "category": category, "from_status": REPLACEMENT, "to_status": SELECTED,
            "actor": "system", "reason": trigger, "at": at, "note": promoted["disposition"],
        })
        _renumber_pool(entries, category)
        _refill_pool(entries, sampling, category, actor, at, events, trigger=trigger)
        return
    if _promote_from_supply(entries, sampling, category, actor, at, supply, events, trigger):
        return
    _bump_pending(entries, sampling, category, 1,
                  reason=f"{trigger} 且替补池为空、跨批无可用候选，缺 1 行待补录", at=at, events=events)


def _refill_pool(entries, sampling, category, actor, at, events, *, trigger):
    """替补池出现空位时，用本批候选顺位补满，保持计划里的替补深度。"""
    depth = int(sampling.get("plan", {}).get("replacement_pool", 0) or 0)
    backlog = sorted(
        (e for e in entries if e["category"] == category and e["status"] == CANDIDATE),
        key=lambda e: (e.get("rank_index") or 10 ** 6, e["row_id"]),
    )
    for entry in backlog[:max(0, depth - len(_replacement_pool(entries, category)))]:
        entry["status"] = REPLACEMENT
        entry["disposition"] = (
            f"替补池补位：{trigger} 后替补池空出名额，本行由候选顺位补入替补池"
        )
        events.append({
            "action": "promote", "run_id": sampling["batch_id"], "row_id": entry["row_id"],
            "category": category, "from_status": CANDIDATE, "to_status": REPLACEMENT,
            "actor": actor, "reason": trigger, "at": at, "note": entry["disposition"],
        })
    _renumber_pool(entries, category)


def _promote_from_supply(entries, sampling, category, actor, at, supply, events, trigger):
    cross = sampling.get("plan", {}).get("cross_batch", {})
    if not cross.get("enabled") or not supply:
        return False
    used = {e["row_id"] for e in entries}
    pool = sorted(
        (s for s in supply if str(s.get("category")) == category and s["row_id"] not in used),
        key=lambda s: (s.get("rank_hash") or "", s["source_batch"], s["row_id"]),
    )
    if not pool:
        return False
    item = pool[0]
    row = item.get("row") or {"row_id": item["row_id"], "category": category}
    columns = list(sampling.get("columns") or row.keys())
    entries.append(_entry(
        row, columns, category, SELECTED,
        rank_hash=item.get("rank_hash") or rank_key(sampling["seed"], item["source_batch"], item["row_id"]),
        row_hash_value=row_hash(row, columns),
        quota=quota_for(sampling["plan"], category), slot=None, rank_index=None,
        origin="cross_batch", source_batch=item["source_batch"],
        disposition=(
            f"撤回补录：{trigger} 且替补池为空，跨批取自批次 {item['source_batch']} 的候选，顶替入选位"
        ),
    ))
    events.append({
        "action": "cross_batch_in", "run_id": sampling["batch_id"], "row_id": item["row_id"],
        "category": category, "from_status": CANDIDATE, "to_status": SELECTED,
        "actor": "system", "reason": trigger, "at": at, "note": entries[-1]["disposition"],
        "target_batch": sampling["batch_id"], "source_batch": item["source_batch"],
    })
    return True


def _bump_pending(entries, sampling, category, missing, *, reason, at, events):
    pending = next((e for e in entries if e["category"] == category and e["status"] == PENDING), None)
    if pending:
        pending["missing_slots"] = int(pending.get("missing_slots") or 0) + missing
        pending["disposition"] = reason
    else:
        entries.append(_pending_entry(category, quota_for(sampling["plan"], category), missing,
                                      reason=reason))
    events.append({
        "action": "pending", "run_id": sampling["batch_id"],
        "row_id": f"{PENDING_ROW_PREFIX}{category}", "category": category,
        "from_status": None, "to_status": PENDING, "actor": "system",
        "reason": reason, "at": at, "note": reason,
    })


# ---------------------------------------------------------------- 重规划（超配/调高）

def replan(sampling: dict, quota_overrides: dict, at: str, actor: str,
           supply: list[dict] | None = None) -> dict:
    """按新配额重排：超配行转入替补池 / 退回来源批次，缺额按替补→候选→跨批→未决补齐。"""
    plan = json.loads(json.dumps(sampling["plan"]))
    old_quotas = {c: quota_for(plan, c) for c in
                  {e.get("category") or "" for e in sampling["entries"] if e.get("category")}}
    for category, value in quota_overrides.items():
        plan["category_quota"][str(category)] = _quota(value)
    sampling["plan"] = plan
    sampling.setdefault("operations", []).append({
        "type": "replan", "quotas": {str(k): _quota(v) for k, v in quota_overrides.items()},
        "actor": actor, "at": at,
    })
    seed = plan["seed"]
    batch_id = sampling["batch_id"]
    entries = [dict(e) for e in sampling["entries"]]
    events: list[dict] = []
    categories = sorted({e.get("category") or "" for e in entries if e.get("category")})

    for category in categories:
        old_quota = old_quotas.get(category, plan["default_quota"])
        new_quota = quota_for(plan, category)
        if new_quota == old_quota:
            continue
        over_cap = new_quota + allowed_over(plan, category)
        selected = sorted(_selected_in(entries, category),
                          key=lambda e: _selection_sort_key(e, seed, batch_id))

        # 1) 超出新配额（含容忍）的入选行：本批行转入替补池，跨批行退回来源批次
        for entry in selected[over_cap:]:
            entry["over_quota"] = True
            if entry.get("origin") == "cross_batch":
                entry["status"] = CANDIDATE
                entry["slot"] = None
                entry["disposition"] = (
                    f"超配退回：配额由 {old_quota} 调整为 {new_quota}，"
                    f"本行来自批次 {entry.get('source_batch')} 的跨批补录，退回来源批次候选池"
                )
                events.append(_over_event(batch_id, entry, category, old_quota, new_quota, actor, at))
            else:
                entry["status"] = REPLACEMENT
                entry["promoted_from"] = SELECTED
                entry["disposition"] = (
                    f"超配转入替补池：配额由 {old_quota} 调整为 {new_quota}，"
                    f"本行原入选但顺位超出，转入 {category} 替补池等待补位"
                )
                events.append(_over_event(batch_id, entry, category, old_quota, new_quota, actor, at))
        _renumber_pool(entries, category)

        # 2) 配额调高：从候选顺位补入
        deficit = new_quota - len(_selected_in(entries, category))
        if deficit > 0:
            backlog = sorted(
                (e for e in entries if e["category"] == category and e["status"] == CANDIDATE),
                key=lambda e: (e.get("rank_index") or 10 ** 6, e["row_id"]),
            )
            for entry in backlog[:deficit]:
                entry["status"] = SELECTED
                entry["promoted_from"] = CANDIDATE
                entry["disposition"] = (
                    f"配额调高补入：{category} 配额由 {old_quota} 调整为 {new_quota}，本行由候选补入选"
                )
                events.append({
                    "action": "promote", "run_id": batch_id, "row_id": entry["row_id"],
                    "category": category, "from_status": CANDIDATE, "to_status": SELECTED,
                    "actor": actor, "reason": f"配额由 {old_quota} 调整为 {new_quota}",
                    "at": at, "note": entry["disposition"],
                })
            deficit -= min(deficit, len(backlog))
        # 3) 仍缺：替补池顶上（例如下调后替补池里有原入选行，先保证配额）
        if deficit > 0:
            for entry in _replacement_pool(entries, category)[:deficit]:
                entry["status"] = SELECTED
                entry["promoted_from"] = REPLACEMENT
                entry["disposition"] = (
                    f"替补入位：配额调整后空出名额，本行由替补池第 {entry.get('slot') or 1} 顺位补入选"
                )
                events.append({
                    "action": "promote", "run_id": batch_id, "row_id": entry["row_id"],
                    "category": category, "from_status": REPLACEMENT, "to_status": SELECTED,
                    "actor": actor, "reason": f"配额调整为 {new_quota}", "at": at,
                    "note": entry["disposition"],
                })
            _renumber_pool(entries, category)
            deficit = new_quota - len(_selected_in(entries, category))
        # 4) 仍缺：跨批补录
        if deficit > 0 and plan["cross_batch"]["enabled"]:
            used = {e["row_id"] for e in entries}
            pool = sorted(
                (s for s in (supply or [])
                 if str(s.get("category")) == category and s["row_id"] not in used),
                key=lambda s: (s.get("rank_hash") or "", s["source_batch"], s["row_id"]),
            )
            for item in pool[:min(deficit, plan["cross_batch"]["max_per_category"])]:
                row = item.get("row") or {"row_id": item["row_id"], "category": category}
                columns = list(item["row"].keys()) if item.get("row") else ["row_id", "category"]
                entries.append(_entry(
                    row, columns, category, SELECTED,
                    rank_hash=item.get("rank_hash") or rank_key(seed, item["source_batch"], item["row_id"]),
                    row_hash_value=row_hash(row, columns),
                    quota=new_quota, slot=None, rank_index=None,
                    origin="cross_batch", source_batch=item["source_batch"],
                    disposition=(
                        f"配额调高跨批补录：{category} 配额 {new_quota}，"
                        f"取自批次 {item['source_batch']} 的候选"
                    ),
                ))
                events.append({
                    "action": "cross_batch_in", "run_id": batch_id, "row_id": item["row_id"],
                    "category": category, "from_status": CANDIDATE, "to_status": SELECTED,
                    "actor": actor, "reason": f"配额调整为 {new_quota}", "at": at,
                    "note": entries[-1]["disposition"],
                    "target_batch": batch_id, "source_batch": item["source_batch"],
                })
            deficit = new_quota - len(_selected_in(entries, category))
        # 5) 未决缺口同步
        pending = next((e for e in entries if e["category"] == category and e["status"] == PENDING), None)
        if deficit > 0:
            if pending:
                pending["missing_slots"] = deficit
                pending["disposition"] = (
                    f"候选不足：按新配额 {new_quota} 尚缺 {deficit} 行；未决，待补录，未齐套不得发布"
                )
            else:
                entries.append(_pending_entry(
                    category, new_quota, deficit,
                    reason=f"候选不足：按新配额 {new_quota} 尚缺 {deficit} 行；未决，待补录",
                ))
        elif pending:
            entries.remove(pending)
            events.append({
                "action": "resolve_pending", "run_id": batch_id, "row_id": pending["row_id"],
                "category": category, "from_status": PENDING, "to_status": None,
                "actor": actor, "reason": f"新配额 {new_quota} 已配齐", "at": at,
                "note": "未决缺口消除",
            })

    sampling["entries"] = entries
    return {"sampling": recount(sampling), "events": events}


def _over_event(batch_id, entry, category, old_quota, new_quota, actor, at) -> dict:
    return {
        "action": "over_quota", "run_id": batch_id, "row_id": entry["row_id"],
        "category": category, "from_status": SELECTED, "to_status": entry["status"],
        "actor": actor, "reason": f"配额由 {old_quota} 调整为 {new_quota}",
        "at": at, "note": entry["disposition"],
    }


def _selection_sort_key(entry: dict, seed: str, batch_id: str) -> tuple:
    if entry.get("origin") == "cross_batch":
        return (1, entry.get("rank_hash") or "", entry.get("source_batch") or "", entry["row_id"])
    return (0, rank_key(seed, batch_id, entry["row_id"]), "", entry["row_id"])


# ---------------------------------------------------------------- 对账

def reconcile(sampling: dict, current_batch_id: str, current_rows: list[dict],
              columns: list[str], stored_fingerprint: str,
              external_batches: dict | None = None,
              events: list[dict] | None = None, at: str = "") -> dict:
    """抽样清单 vs 源数据逐项对账；全部 pass 才 ``status=complete``（允许发布）。"""
    external_batches = external_batches or {}
    entries = sampling["entries"]
    checks: list[dict] = []

    def check(name: str, ok: bool, detail: str) -> None:
        checks.append({"check": name, "status": "pass" if ok else "fail", "detail": detail})

    current_fp = fingerprint(current_rows, columns)
    check("源数据指纹一致", current_fp == stored_fingerprint,
          "源数据未变化，指纹 " + current_fp[:16] if current_fp == stored_fingerprint
          else f"源数据指纹由 {stored_fingerprint[:16]} 变为 {current_fp[:16]}，清单需重算")

    batches = {current_batch_id: {"rows": current_rows, "columns": columns,
                                  "fingerprint": stored_fingerprint}}
    batches.update(external_batches)

    missing_rows: list[str] = []
    category_mismatch: list[dict] = []
    hash_mismatch: list[str] = []
    unknown_batch: list[dict] = []
    checked = 0
    for entry in entries:
        if _is_pending(entry):
            continue
        checked += 1
        source_batch = entry.get("source_batch") or current_batch_id
        batch = batches.get(source_batch)
        if batch is None:
            unknown_batch.append({"row_id": entry["row_id"], "source_batch": source_batch})
            continue
        row = next((r for r in batch["rows"] if r.get("row_id") == entry["row_id"]), None)
        if row is None:
            missing_rows.append(entry["row_id"])
            continue
        if str(row.get("category", "") or "").strip() != entry["category"]:
            category_mismatch.append({"row_id": entry["row_id"], "expected": entry["category"],
                                      "actual": row.get("category", "")})
        if row_hash(row, batch["columns"]) != entry["row_hash"]:
            hash_mismatch.append(entry["row_id"])

    check("清单行均存在于源数据", not missing_rows,
          f"共校验 {checked} 行，全部可定位" if not missing_rows
          else "源数据缺失行：" + "、".join(missing_rows[:10]))
    check("行品类与源数据一致", not category_mismatch,
          "全部一致" if not category_mismatch
          else "不一致：" + json.dumps(category_mismatch[:5], ensure_ascii=False))
    check("行哈希与源数据一致", not hash_mismatch,
          "全部一致" if not hash_mismatch else "哈希不一致行：" + "、".join(hash_mismatch[:10]))
    check("跨批补录来源批次可定位", not unknown_batch,
          "无跨批补录行或来源批次均已载入" if not unknown_batch
          else "来源批次缺失：" + json.dumps(unknown_batch[:5], ensure_ascii=False))
    for batch_id, batch in batches.items():
        if batch_id == current_batch_id:
            continue
        live = batch.get("current_fingerprint") or fingerprint(batch["rows"], batch["columns"])
        ok = live == batch["fingerprint"]
        check(f"来源批次 {batch_id} 指纹未漂移", ok,
              "一致" if ok else "来源批次源数据已变化，跨批补录行不可用")

    selected_ids = [e["row_id"] for e in entries if e["status"] == SELECTED]
    dupes = sorted({rid for rid in selected_ids if selected_ids.count(rid) > 1})
    check("入选行无重复", not dupes,
          "入选行唯一" if not dupes else "重复入选：" + "、".join(dupes))

    quotas: list[dict] = []
    for cat in sampling.get("categories", []):
        category = cat["category"]
        quota = quota_for(sampling["plan"], category)
        cap = quota + allowed_over(sampling["plan"], category)
        count = len([e for e in entries if e["category"] == category and e["status"] == SELECTED])
        pending_slots = sum(int(e.get("missing_slots") or 0) for e in entries
                            if e["category"] == category and e["status"] == PENDING)
        ok = count >= quota and count <= cap and pending_slots == 0
        if count < quota:
            detail = f"配额 {quota}，入选 {count}，尚缺 {quota - count} 行" + (
                f"（未决 {pending_slots} 行）" if pending_slots else "")
        elif count > cap:
            detail = f"配额 {quota}（容忍 {allowed_over(sampling['plan'], category)}），入选 {count} 超配"
        elif pending_slots:
            detail = f"配额 {quota}，入选 {count}，但仍有未决 {pending_slots} 行"
        else:
            detail = f"配额 {quota}，入选 {count}，已配齐"
        quotas.append({"category": category, "quota": quota, "cap": cap,
                       "selected": count, "pending_slots": pending_slots,
                       "status": "pass" if ok else "fail", "detail": detail})
        check(f"品类「{category}」配额配齐", ok, detail)

    withdrawn = [e for e in entries if e["status"] == WITHDRAWN]
    withdraw_events = {e["row_id"] for e in (events or []) if e.get("action") == "withdraw"}
    unaudited = [e["row_id"] for e in withdrawn if e["row_id"] not in withdraw_events]
    check("撤回行均有审计事件", not unaudited,
          f"撤回 {len(withdrawn)} 行，审计事件齐全" if not unaudited
          else "缺少撤回审计：" + "、".join(unaudited))

    status = "complete" if all(c["status"] == "pass" for c in checks) else "mismatch"
    return {
        "status": status,
        "checks": checks,
        "quotas": quotas,
        "checked_at": at,
        "batch_id": current_batch_id,
        "fingerprint": current_fp,
        "stored_fingerprint": stored_fingerprint,
        "counts": sampling.get("counts", {}),
        "blockers": [c["detail"] for c in checks if c["status"] == "fail"],
    }


# ---------------------------------------------------------------- 重放

def replay(sampling: dict, rows: list[dict], columns: list[str]) -> dict:
    """按存储的计划/种子/候选池快照重算清单，再按审计操作重放撤回/重规划，逐行比较。"""
    plan = sampling["replay_input"]["plan"]
    batch_id = sampling["replay_input"]["batch_id"]
    supply = sampling["replay_input"]["supply"]
    fresh = select(rows, columns, plan, batch_id, supply)
    replayed = fresh
    for operation in sampling.get("operations") or []:
        if operation["type"] == "withdraw":
            replayed = withdraw(replayed, operation["row_id"], operation["actor"],
                                operation["reason"], operation["at"], supply)["sampling"]
        elif operation["type"] == "replan":
            replayed = replan(replayed, operation["quotas"], operation["at"],
                              operation["actor"], supply)["sampling"]
    stored = sampling["entries"]

    def ids(source: list[dict], status: str) -> set[str]:
        return {e["row_id"] for e in source if e["status"] == status}

    diffs = []
    for status in (SELECTED, REPLACEMENT, CANDIDATE, WITHDRAWN, PENDING):
        expected, actual = ids(replayed["entries"], status), ids(stored, status)
        if expected != actual:
            diffs.append({"status": status,
                          "missing_in_list": sorted(expected - actual)[:20],
                          "extra_in_list": sorted(actual - expected)[:20]})

    stored_hashes = {e["row_id"]: e["row_hash"] for e in stored
                     if e["status"] in (SELECTED, REPLACEMENT) and e["row_hash"]}
    replayed_hashes = {e["row_id"]: e["row_hash"] for e in replayed["entries"]
                       if e["status"] in (SELECTED, REPLACEMENT) and e["row_hash"]}
    hash_diffs = sorted(rid for rid in set(stored_hashes) & set(replayed_hashes)
                        if stored_hashes[rid] != replayed_hashes[rid])

    return {
        "matched": not diffs and not hash_diffs,
        "diffs": diffs,
        "hash_diffs": hash_diffs,
        "replayed_selected": len(ids(replayed["entries"], SELECTED)),
        "stored_selected": len(ids(stored, SELECTED)),
        "operations_replayed": len(sampling.get("operations") or []),
        "fingerprint": fingerprint(rows, columns),
        "seed": plan["seed"],
        "batch_id": batch_id,
        "algorithm": "sha256(seed|batch_id|row_id) 升序，按品类配额取前 N，再按审计操作重放",
    }
