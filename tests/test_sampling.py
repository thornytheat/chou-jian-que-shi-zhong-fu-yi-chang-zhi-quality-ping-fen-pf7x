"""抽样配额：可重放、候选不足、跨批补录、超配去向、撤回审计与对账闸门。"""
from __future__ import annotations

from app import sampling


def rows_of(categories: dict[str, int], prefix: str = "R") -> list[dict]:
    rows, seq = [], 0
    for category, count in categories.items():
        for _ in range(count):
            seq += 1
            rows.append({"row_id": f"{prefix}{seq:04d}", "product_id": f"P{seq:04d}",
                         "category": category, "price": "10.00", "stock": "5",
                         "weight": "1.20", "supplier": "S01",
                         "updated_at": "2026-09-01", "rating": "4.5"})
    return rows


COLUMNS = list(rows_of({"x": 1})[0].keys())
PLAN = {"seed": "s", "default_quota": 2, "category_quota": {}, "replacement_pool": 2,
        "cross_batch": {"enabled": True, "max_per_category": 100},
        "over_quota_allowance": 0.0}


def test_sampling_is_replayable_regardless_of_row_order():
    rows = rows_of({"A": 10, "B": 10})
    first = sampling.select(rows, COLUMNS, PLAN, "B1")
    shuffled = list(reversed(rows))
    second = sampling.select(shuffled, COLUMNS, PLAN, "B1")
    selected_first = [e["row_id"] for e in first["entries"] if e["status"] == "selected"]
    selected_second = [e["row_id"] for e in second["entries"] if e["status"] == "selected"]
    assert selected_first == selected_second
    assert first["fingerprint"] == second["fingerprint"]
    replay = sampling.replay(first, rows, COLUMNS)
    assert replay["matched"] is True


def test_quota_selected_replacement_and_candidate_are_partitioned():
    rows = rows_of({"A": 10})
    plan = {**PLAN, "category_quota": {"A": 3}, "replacement_pool": 2}
    result = sampling.select(rows, COLUMNS, plan, "B1")
    statuses = [e["status"] for e in result["entries"] if e["category"] == "A"]
    assert statuses.count("selected") == 3
    assert statuses.count("replacement") == 2
    assert statuses.count("candidate") == 5
    pool = [e for e in result["entries"] if e["status"] == "replacement"]
    assert [e["slot"] for e in pool] == [1, 2]
    assert all("替补池第" in e["disposition"] for e in pool)


def test_candidate_shortfall_creates_pending_and_blocks_reconcile():
    rows = rows_of({"A": 10, "tiny": 2})
    plan = {**PLAN, "category_quota": {"tiny": 5, "A": 3}}
    result = sampling.select(rows, COLUMNS, plan, "B1")
    pending = [e for e in result["entries"] if e["status"] == "pending"]
    assert len(pending) == 1
    assert pending[0]["missing_slots"] == 3
    reconcile = sampling.reconcile(result, "B1", rows, COLUMNS, result["fingerprint"])
    assert reconcile["status"] == "mismatch"
    failed = [q for q in reconcile["quotas"] if q["status"] == "fail"]
    assert failed and failed[0]["category"] == "tiny"
    assert failed[0]["pending_slots"] == 3


def test_cross_batch_fill_records_source_batch_and_reconciles():
    prior_rows = rows_of({"A": 10}) + [r for r in rows_of({"tiny": 5}, "Z")]
    plan_prior = {**PLAN, "category_quota": {"tiny": 1}}
    prior = sampling.select(prior_rows, COLUMNS, plan_prior, "PRIOR")
    supply = [{"row_id": e["row_id"], "category": e["category"],
               "source_batch": "PRIOR", "rank_hash": e["rank_hash"],
               "rank_index": e["rank_index"], "row": e["row"]}
              for e in prior["entries"] if e["status"] in ("candidate", "replacement")]
    # 本批只有 1 行 tiny，配额 3 → 缺 2 行由 PRIOR 候选补录
    current = rows_of({"A": 10}) + [r for r in rows_of({"tiny": 5}, "Z")][:1]
    plan = {**PLAN, "category_quota": {"tiny": 3}}
    result = sampling.select(current, COLUMNS, plan, "CURRENT", supply)
    cross = [e for e in result["entries"] if e.get("origin") == "cross_batch"]
    assert len(cross) == 2, "候选不足时应跨批补录 2 行"
    assert all(e["source_batch"] == "PRIOR" for e in cross)
    assert all("跨批补录" in e["disposition"] for e in cross)
    external = {"PRIOR": {"rows": prior_rows, "columns": COLUMNS,
                          "fingerprint": prior["fingerprint"]}}
    reconcile = sampling.reconcile(result, "CURRENT", current, COLUMNS,
                                   result["fingerprint"], external_batches=external)
    assert reconcile["status"] == "complete", reconcile["blockers"]
    replay = sampling.replay(result, current, COLUMNS)
    assert replay["matched"] is True
    # 来源批次源数据漂移会被对账抓住
    drifted = [dict(r) for r in prior_rows]
    drifted[0]["price"] = "999.00"
    external_drift = {"PRIOR": {"rows": drifted, "columns": COLUMNS,
                                "fingerprint": prior["fingerprint"]}}
    assert sampling.reconcile(result, "CURRENT", current, COLUMNS,
                              result["fingerprint"],
                              external_batches=external_drift)["status"] == "mismatch"


def test_withdraw_keeps_audit_and_promotes_replacement():
    rows = rows_of({"A": 10})
    plan = {**PLAN, "category_quota": {"A": 3}, "replacement_pool": 2}
    result = sampling.select(rows, COLUMNS, plan, "B1")
    victim = next(e for e in result["entries"] if e["status"] == "selected")
    out = sampling.withdraw(result, victim["row_id"], "审计员", "录入错误", "2026-11-14")
    updated = out["sampling"]
    assert victim["row_id"] in {e["row_id"] for e in updated["entries"]
                                if e["status"] == "withdrawn"}
    actions = [e["action"] for e in out["events"]]
    assert actions[0] == "withdraw"
    assert "promote" in actions
    selected_before = 3
    assert updated["counts"]["selected"] == selected_before
    assert updated["counts"]["withdrawn"] == 1
    reconcile = sampling.reconcile(updated, "B1", rows, COLUMNS,
                                   updated["fingerprint"], events=out["events"])
    assert reconcile["status"] == "complete", reconcile["blockers"]
    # 无审计事件则不齐套（撤回痕迹不能丢）
    without = sampling.reconcile(updated, "B1", rows, COLUMNS, updated["fingerprint"])
    assert without["status"] == "mismatch"


def test_replan_down_demotes_over_quota_with_disposition():
    rows = rows_of({"A": 10})
    plan = {**PLAN, "category_quota": {"A": 5}, "replacement_pool": 1}
    result = sampling.select(rows, COLUMNS, plan, "B1")
    out = sampling.replan(result, {"A": 2}, "2026-11-14", "复核人")
    updated = out["sampling"]
    assert updated["plan"]["category_quota"]["A"] == 2
    assert updated["counts"]["selected"] == 2
    demoted = [e for e in updated["entries"] if e.get("over_quota")
               and e["status"] == "replacement"]
    assert len(demoted) == 3
    assert all("超配转入替补池" in e["disposition"] for e in demoted)
    over_events = [e for e in out["events"] if e["action"] == "over_quota"]
    assert len(over_events) == 3
    assert sampling.replay(updated, rows, COLUMNS)["matched"] is True
    assert sampling.reconcile(updated, "B1", rows, COLUMNS, updated["fingerprint"],
                              events=out["events"])["status"] == "complete"


def test_replan_up_fills_from_backlog():
    rows = rows_of({"A": 10})
    plan = {**PLAN, "category_quota": {"A": 2}, "replacement_pool": 0}
    result = sampling.select(rows, COLUMNS, plan, "B1")
    out = sampling.replan(result, {"A": 5}, "2026-11-14", "复核人")
    assert out["sampling"]["counts"]["selected"] == 5
    promoted = [e for e in out["events"] if e["action"] == "promote"]
    assert len(promoted) == 3
    assert sampling.replay(out["sampling"], rows, COLUMNS)["matched"] is True


def test_source_drift_flips_reconcile_to_mismatch():
    rows = rows_of({"A": 5})
    plan = {**PLAN, "category_quota": {"A": 3}}
    result = sampling.select(rows, COLUMNS, plan, "B1")
    changed = [dict(row) for row in rows]
    changed[0]["price"] = "11.00"
    reconcile = sampling.reconcile(result, "B1", changed, COLUMNS,
                                   result["fingerprint"], at="t")
    assert reconcile["status"] == "mismatch"
    assert any("指纹" in blocker or "哈希" in blocker for blocker in reconcile["blockers"])
