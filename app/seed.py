"""幂等 seed：首次启动用样例数据跑一次完整抽样 → 对账 → 评分 → 发布。

幂等口径：settings.seed_version 记录已完成的 seed 版本，重复启动不再插入新记录。
seed 也走标准编排（抽样 + 对账 + 断点），保证样例与手工运行的发布口径一致。
"""
from __future__ import annotations

from . import loader, orchestrate, sampling as sampling_mod, settings, store

SEED_RUN_ID = "seed-sample"
SEED_BATCH_ID = "seed-batch"


def ensure_seed() -> dict:
    store.connect()
    rules = loader.load_rules()
    current = store.get_setting("seed_version")
    if current == settings.SEED_VERSION and store.get_run(SEED_RUN_ID):
        return {"seeded": False, "run_id": SEED_RUN_ID, "rules": rules}
    if store.get_run(SEED_RUN_ID) is None:
        data = loader.load_sample(rules)
        rows, columns = data["rows"], data["columns"]
        store.save_batch(SEED_BATCH_ID, rows, columns,
                         sampling_mod.fingerprint(rows, columns), kind="sample")
        store.create_run(SEED_RUN_ID, data["source"], store.weight_overrides(),
                         batch_id=SEED_BATCH_ID)
        orchestrate.execute(SEED_RUN_ID)
        run = store.get_run(SEED_RUN_ID) or {}
        report = run.get("report") or {}
        if report:
            report["dataset"]["source"] = data["source"]
            report["dataset"]["encoding"] = data.get("encoding", "utf-8")
            store.update_report(SEED_RUN_ID, report, store.weight_overrides())
    store.set_setting("seed_version", settings.SEED_VERSION)
    return {"seeded": True, "run_id": SEED_RUN_ID, "rules": rules}
