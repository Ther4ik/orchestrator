"""Имитация исполнения расчётов (asyncio). Реальные алгоритмы подключаются вместо _run()."""
import asyncio

from .models import Status, Task
from .orchestrator import Orchestrator

STAGES = ["load", "validate", "calculate", "write_staging", "publish"]


class SimulatedRunner:
    def __init__(self, orch: Orchestrator, step_seconds: float = 1.0):
        self.orch, self.step = orch, step_seconds
        orch.on_start = self.start

    def start(self, task: Task):
        asyncio.get_running_loop().create_task(self._run(task.id))

    async def _run(self, task_id: str):
        task = self.orch.get(task_id)
        for i, stage in enumerate(STAGES, 1):
            await asyncio.sleep(self.step)
            if task.status != Status.RUNNING:       # отменили во время работы
                return
            if task.simulate_failure_at == stage:
                self.orch.fail(task_id, f"Имитация сбоя на этапе «{stage}» (обрыв соединения с БД)")
                return
            self.orch.report_progress(task_id, stage, i / len(STAGES))
        self.orch.complete(task_id)
