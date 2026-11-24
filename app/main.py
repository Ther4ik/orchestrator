"""REST API. Запуск:  uvicorn app.main:app --reload   (документация: http://localhost:8000/docs)"""
from datetime import date
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .catalog import PROCESSES
from .models import SourceStatus, Status, Trigger
from .orchestrator import Orchestrator
from .runner import STAGES, SimulatedRunner

STEP_SECONDS = 2.0   # длительность одного этапа имитации
# Эндпоинты объявлены async: ядро не блокирует (всё в памяти) и должно работать в потоке event loop,
# где живёт исполнитель (asyncio.create_task).

app = FastAPI(title="GARAZ Risk Orchestrator (MVP)")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

orch = Orchestrator()
runner = SimulatedRunner(orch, STEP_SECONDS)


class RunRequest(BaseModel):
    process_id: str
    business_date: date
    trigger: Trigger = Trigger.MANUAL
    requested_by: str = "user"
    simulate_failure_at: Optional[str] = None   # одно из: load, validate, calculate, write_staging, publish


class SourceUpdate(BaseModel):
    mode: str = "versioned"   # versioned | exclusive


class SourceStatusUpdate(BaseModel):
    status: SourceStatus


def _task(t):
    return t.to_dict(orch.current_versions())


@app.get("/health")
async def health():
    return {"ok": True}


# ---- справочники
@app.get("/processes")
async def processes():
    return [{"id": p.id, "name": p.name, "department": p.department, "schedule": p.schedule, "mode": p.mode,
             "weight": p.weight.name, "group": p.group, "sources": list(p.sources), "emergency": p.emergency}
            for p in PROCESSES.values()]


@app.get("/sources")
async def sources():
    return [s.to_dict() for s in orch.sources.values()]


# ---- задачи
@app.post("/tasks", status_code=201)
async def create_task(req: RunRequest):
    if req.simulate_failure_at and req.simulate_failure_at not in STAGES:
        raise HTTPException(422, f"simulate_failure_at: один из {STAGES}")
    try:
        task, decision = orch.submit(req.process_id, req.business_date, req.trigger, req.requested_by,
                                     req.simulate_failure_at)
    except ValueError as e:
        raise HTTPException(404, str(e))
    return {"task": _task(task), "decision": decision.to_dict()}


@app.get("/tasks")
async def list_tasks(status: Optional[Status] = None, process_id: Optional[str] = None):
    items = sorted(orch.tasks.values(), key=lambda t: t.seq, reverse=True)
    return [_task(t) for t in items
            if (status is None or t.status == status) and (process_id is None or t.process_id == process_id)]


@app.get("/tasks/{task_id}")
async def get_task(task_id: str):
    try:
        return _task(orch.get(task_id))
    except KeyError:
        raise HTTPException(404, "Задача не найдена")


@app.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    try:
        return _task(orch.cancel(task_id))
    except KeyError:
        raise HTTPException(404, "Задача не найдена")
    except ValueError as e:
        raise HTTPException(409, str(e))


@app.post("/tasks/{task_id}/retry", status_code=201)
async def retry_task(task_id: str):
    try:
        task, decision = orch.retry(task_id)
    except KeyError:
        raise HTTPException(404, "Задача не найдена")
    except ValueError as e:
        raise HTTPException(409, str(e))
    return {"task": _task(task), "decision": decision.to_dict()}


# ---- очередь, нагрузка, история
@app.get("/queue")
async def queue():
    return [_task(t) for t in orch.queue()]


@app.get("/system/load")
async def system_load():
    return orch.load()


@app.get("/results")
async def results():
    return list(orch.results.values())


@app.get("/audit")
async def audit(limit: int = 200):
    return orch.audit[-limit:]


# ---- управление источниками (имитация внешних событий для демо)
@app.post("/sources/{source_id}/update")
async def update_source(source_id: str, body: SourceUpdate):
    try:
        return orch.update_source(source_id, body.mode)
    except KeyError:
        raise HTTPException(404, "Источник не найден")
    except ValueError as e:
        raise HTTPException(422, str(e))


@app.put("/sources/{source_id}/status")
async def set_source_status(source_id: str, body: SourceStatusUpdate):
    try:
        orch.set_source_status(source_id, body.status)
    except KeyError:
        raise HTTPException(404, "Источник не найден")
    return orch.sources[source_id].to_dict()


# ---- веб-интерфейс: http://localhost:8000/ui/  (монтируем последним, чтобы не перекрыть API)
app.mount("/ui", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="ui")
