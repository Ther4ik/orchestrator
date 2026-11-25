"""Консольное демо без FastAPI: python demo.py — печатает порядок выполнения операций."""
import asyncio
from datetime import date

from app.models import Trigger
from app.orchestrator import Orchestrator
from app.runner import SimulatedRunner

D = date(2026, 11, 23)


async def main():
    orch = Orchestrator()
    SimulatedRunner(orch, step_seconds=0.2)
    orch.submit("CR-01", D, Trigger.SCHEDULED, "scheduler")           # К-01: плановый ECL
    orch.submit("CR-07", D, Trigger.MANUAL, "ivanov")                  # К-01: ручной пересчёт -> очередь
    orch.submit("MR-01", D, Trigger.SCHEDULED, "scheduler")            # К-04: VaR
    await asyncio.sleep(0.3)
    orch.update_source("SRC-006", "versioned")                         # К-04: новые котировки во время расчёта
    orch.submit("LR-03", D, Trigger.SCHEDULED, "scheduler", simulate_failure_at="calculate")  # К-06: сбой
    await asyncio.sleep(3)
    failed = next(t for t in orch.tasks.values() if t.process_id == "LR-03")
    orch.retry(failed.id)
    await asyncio.sleep(2)

    print(f"{'время':<13}{'событие':<18}{'задача':<8}{'процесс':<8}детали")
    for e in orch.audit:
        print(f"{e['ts'][11:23]:<13}{e['event']:<18}{(e['task_id'] or '-'):<8}{(e['process_id'] or '-'):<8}{e['text']}")


if __name__ == "__main__":
    asyncio.run(main())
