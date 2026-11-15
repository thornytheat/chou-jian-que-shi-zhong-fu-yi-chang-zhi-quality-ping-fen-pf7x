"""抽样闸门与发布流程的接口测试：候选/入选/替补/未决、撤回审计、重规划、重放与续跑。"""
from __future__ import annotations

from conftest import poll

HEADER = "row_id,product_id,category,price,stock,weight,supplier,updated_at,rating\n"


def csv_for(categories: dict[str, int]) -> str:
    lines, seq = [HEADER], 0
    for category, count in categories.items():
        for _ in range(count):
            seq += 1
            lines.append(
                f"R{seq:04d},P{seq:04d},{category},100.00,{10 + seq},1.20,S01,2026-09-01,4.5\n"
            )
    return "".join(lines)


def start(client, categories, quota=None, **extra):
    payload = {"csv_text": csv_for(categories)}
    if quota:
        payload["category_quota"] = quota
    payload.update(extra)
    res = client.post("/api/runs/text", json=payload)
    assert res.status_code == 200, res.text
    return res.json()["run_id"]


def test_run_publishes_sampling_and_reconcile(client):
    run_id = start(client, {"数码配件": 40, "家居日用": 30},
                   {"数码配件": 5, "家居日用": 3})
    status = poll(client, run_id)
    assert status["status"] == "done"
    assert status["reconcile_status"] == "complete"
    assert status["publish_status"] == "published"
    assert status["sampled_rows"] == 8
    assert status["sampling_counts"]["selected"] == 8
    assert status["sampling_counts"]["replacement"] == 4

    sampling = client.get(f"/api/runs/{run_id}/sampling").json()
    assert sampling["counts"]["selected"] == 8
    statuses = {e["status"] for e in sampling["entries"]}
    assert {"selected", "replacement", "candidate"} <= statuses
    assert all(e["disposition"] for e in sampling["entries"])

    replay = client.post(f"/api/runs/{run_id}/sampling/replay").json()
    assert replay["matched"] is True

    assert client.get(f"/api/runs/{run_id}/reconcile").json()["reconcile_status"] == "complete"
    assert "入选" in client.get(f"/api/runs/{run_id}/sampling.csv").text


def test_candidate_shortfall_blocks_publication_then_replan_fixes(client):
    # 用种子样例里不存在的品类，确保没有跨批候选可补，缺口保持未决
    run_id = start(client, {"数码配件": 40, "孤品": 2},
                   {"数码配件": 3, "孤品": 5})
    status = poll(client, run_id)
    assert status["status"] == "blocked"
    assert status["publish_status"] == "withheld"
    assert status["pending_slots"] == 3
    # 未齐套：结果页是「未发布」，工单与重算都被 409 拒绝
    page = client.get(f"/result/{run_id}")
    assert page.status_code == 200 and "未发布" in page.text
    assert client.get(f"/api/runs/{run_id}/alerts.csv").status_code == 409
    assert client.post(f"/api/runs/{run_id}/rescore").status_code == 409

    replan = client.post(f"/api/runs/{run_id}/sampling/replan",
                         json={"category_quota": {"孤品": 2}, "actor": "复核人"})
    assert replan.status_code == 200
    assert replan.json()["counts"]["pending_slots"] == 0

    publish = client.post(f"/api/runs/{run_id}/publish")
    assert publish.status_code == 200
    status = poll(client, run_id)
    assert status["status"] == "done"
    assert status["publish_status"] == "published"
    assert client.get(f"/api/runs/{run_id}/alerts.csv").status_code == 200


def test_withdraw_keeps_audit_and_requires_republish(client):
    run_id = start(client, {"数码配件": 40}, {"数码配件": 5})
    poll(client, run_id)
    sampling = client.get(f"/api/runs/{run_id}/sampling?status=selected&limit=1").json()
    row_id = sampling["entries"][0]["row_id"]

    missing_reason = client.post(f"/api/runs/{run_id}/sampling/withdraw",
                                 json={"row_id": row_id, "actor": "审计员"})
    assert missing_reason.status_code == 400

    res = client.post(f"/api/runs/{run_id}/sampling/withdraw",
                      json={"row_id": row_id, "actor": "审计员", "reason": "上游撤回录入"})
    assert res.status_code == 200
    body = res.json()
    assert body["counts"]["withdrawn"] == 1
    assert body["publish_status"] == "republish_required"
    actions = [e["action"] for e in body["events"]]
    assert actions[0] == "withdraw"
    assert "promote" in actions

    # 清单变更后立即重新对账：齐套（替补顶上）→ 但要重新计算才发布
    reconcile = client.get(f"/api/runs/{run_id}/reconcile").json()
    assert reconcile["reconcile_status"] == "complete"
    assert reconcile["publish_status"] == "republish_required"
    # 已发布旧结果不再可下载，直到重新发布
    assert client.get(f"/api/runs/{run_id}/alerts.csv").status_code == 409

    audit = client.get(f"/api/runs/{run_id}/audit").json()
    assert audit["total"] >= 2
    withdraw_event = next(e for e in audit["events"] if e["action"] == "withdraw")
    assert withdraw_event["actor"] == "审计员"
    assert withdraw_event["reason"] == "上游撤回录入"
    withdrawn = [e for e in client.get(f"/api/runs/{run_id}/sampling").json()["entries"]
                 if e["status"] == "withdrawn"]
    assert withdrawn and "已撤回" in withdrawn[0]["disposition"]

    # 重复撤回被拒绝（审计痕迹已存在）
    duplicate = client.post(f"/api/runs/{run_id}/sampling/withdraw",
                            json={"row_id": row_id, "actor": "审计员", "reason": "再撤一次"})
    assert duplicate.status_code == 409

    publish = client.post(f"/api/runs/{run_id}/publish")
    assert publish.status_code == 200
    status = poll(client, run_id)
    assert status["publish_status"] == "published"


def test_over_quota_replan_marks_disposition(client):
    run_id = start(client, {"数码配件": 40}, {"数码配件": 6})
    poll(client, run_id)
    res = client.post(f"/api/runs/{run_id}/sampling/replan",
                      json={"category_quota": {"数码配件": 2}, "actor": "复核人"})
    assert res.status_code == 200
    body = res.json()
    assert body["counts"]["selected"] == 2
    over = [e for e in res.json()["events"] if e["action"] == "over_quota"]
    assert len(over) == 4
    assert all("超配" in e["note"] for e in over)
    entries = client.get(f"/api/runs/{run_id}/sampling").json()["entries"]
    demoted = [e for e in entries if e.get("over_quota") and e["status"] == "replacement"]
    assert len(demoted) == 4
    assert all("超配转入替补池" in e["disposition"] for e in demoted)


def test_resume_after_failure_skips_confirmed_stages(client, monkeypatch):
    from app import alerts, store

    run_id = start(client, {"数码配件": 40}, {"数码配件": 5})
    # 抢在后台线程执行前打断：直接插入一次「已确认前 6 阶段」的断点后重跑
    original_build = alerts.build
    monkeypatch.setattr(alerts, "build", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("模拟失败")))
    # 用同一批次新起一个运行，令其在中途失败
    batch = store.get_run(run_id)["batch_id"]
    fail_id = "fail-" + run_id
    store.create_run(fail_id, "重跑", {}, batch_id=batch)
    try:
        from app import orchestrate
        orchestrate.execute(fail_id)
    except RuntimeError:
        pass
    monkeypatch.setattr(alerts, "build", original_build)
    run = store.get_run(fail_id)
    assert run["checkpoint"]["stage"] == "score"
    assert "score" in run["checkpoint"]["state"]

    store.set_run_state(fail_id, "failed", "模拟失败", run["stage_index"], error="模拟失败")
    res = client.post(f"/api/runs/{fail_id}/resume")
    assert res.status_code == 200
    status = poll(client, fail_id)
    assert status["status"] == "done"
    assert status["publish_status"] == "published"


def test_batch_label_conflict_rejected(client):
    payload = {"csv_text": csv_for({"数码配件": 5}), "batch": "2026-11"}
    assert client.post("/api/runs/text", json=payload).status_code == 200
    conflict = client.post("/api/runs/text",
                           json={"csv_text": csv_for({"数码配件": 6}), "batch": "2026-11"})
    assert conflict.status_code == 409
    assert "批次" in conflict.json()["detail"]


def test_monthly_cross_batch_fill_via_api(client):
    def csv(categories, prefix):
        lines, seq = [HEADER], 0
        for category, count in categories.items():
            for _ in range(count):
                seq += 1
                lines.append(
                    f"{prefix}{seq:04d},P{prefix}{seq:04d},{category},100.00,{seq},1.20,S01,2026-09-01,4.5\n"
                )
        return "".join(lines)

    october = client.post("/api/runs/text", json={
        "csv_text": csv({"A": 10, "B": 10}, "O"),
        "batch": "cross-2026-10", "category_quota": {"A": 3, "B": 3},
    }).json()["run_id"]
    assert poll(client, october)["publish_status"] == "published"

    november = client.post("/api/runs/text", json={
        "csv_text": csv({"A": 1, "C": 5}, "N"),
        "batch": "cross-2026-11", "category_quota": {"A": 5, "C": 3},
    }).json()["run_id"]
    status = poll(client, november)
    assert status["status"] == "done"
    assert status["sampling_counts"]["cross_batch_in"] == 4
    assert status["pending_slots"] == 0

    sampling = client.get(f"/api/runs/{november}/sampling").json()
    cross = [e for e in sampling["entries"] if e.get("origin") == "cross_batch"]
    assert all(e["source_batch"] == "cross-2026-10" for e in cross)
    assert all("跨批补录" in e["disposition"] for e in cross)
    checks = client.get(f"/api/runs/{november}/reconcile").json()["reconcile"]["checks"]
    assert any(c["check"].startswith("来源批次 cross-2026-10") and c["status"] == "pass" for c in checks)
