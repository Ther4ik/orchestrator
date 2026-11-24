"""Доменные модели оркестратора (без зависимостей от FastAPI)."""
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Optional


def now() -> datetime:
    return datetime.now(timezone.utc)


class Status(str, Enum):
    QUEUED = "Выполнение ожидает"
    RUNNING = "Выполняется"
    SUCCESS = "Успешно"
    FAILED = "Ошибка"
    CANCELLED = "Отменено"
    REJECTED = "Отклонено"


ACTIVE = (Status.QUEUED, Status.RUNNING)


class Trigger(str, Enum):
    SCHEDULED = "scheduled"
    MANUAL = "manual"


class Priority(int, Enum):  # меньше число — выше приоритет (К-07, вариант В)
    EMERGENCY = 0
    SCHEDULED = 1
    MANUAL = 2


class SourceStatus(str, Enum):
    READY = "Готов"
    UPDATING = "Обновляется"
    INCOMPLETE = "Неполные данные"
    ERROR = "Ошибка"


class Verdict(str, Enum):
    RUN = "run"
    QUEUE = "queue"
    REJECT = "reject"


@dataclass
class Reason:
    code: str                       # К-01 … К-07
    text: str
    blocked_by: Optional[str] = None  # id задачи, из-за которой ждём

    def to_dict(self):
        return {"code": self.code, "text": self.text, "blocked_by": self.blocked_by}


@dataclass
class Decision:
    verdict: Verdict
    reasons: list = field(default_factory=list)

    def to_dict(self):
        return {"verdict": self.verdict.value, "reasons": [r.to_dict() for r in self.reasons]}


@dataclass
class Source:
    id: str
    name: str
    status: SourceStatus = SourceStatus.READY
    version: int = 1
    updated_at: datetime = field(default_factory=now)
    readers: set = field(default_factory=set)   # id задач, читающих источник сейчас
    pending_exclusive: bool = False             # ждёт эксклюзивной записи (блокирует новых читателей)

    def to_dict(self):
        return {"id": self.id, "name": self.name, "status": self.status.value, "version": self.version,
                "updated_at": self.updated_at.isoformat(), "readers": sorted(self.readers),
                "pending_exclusive_update": self.pending_exclusive}


@dataclass
class Task:
    """Конкретный запуск процесса (экземпляр), отдельная сущность от шаблона процесса."""
    id: str
    seq: int
    process_id: str
    business_date: date
    trigger: Trigger
    priority: Priority
    requested_by: str
    attempt: int = 1
    retry_of: Optional[str] = None
    simulate_failure_at: Optional[str] = None
    status: Status = Status.QUEUED
    reasons: list = field(default_factory=list)       # почему ждёт / отклонено
    snapshot: dict = field(default_factory=dict)      # {source_id: version} — фиксируется на старте (К-04)
    staging: Optional[str] = None                     # изолированная область записи (К-03, К-06)
    stage: Optional[str] = None
    progress: float = 0.0
    error: Optional[str] = None
    created_at: datetime = field(default_factory=now)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    def to_dict(self, current_versions: Optional[dict] = None):
        drift = {}
        if current_versions and self.status == Status.RUNNING:
            drift = {s: {"used": v, "current": current_versions[s]}
                     for s, v in self.snapshot.items() if current_versions[s] != v}
        return {
            "id": self.id, "process_id": self.process_id, "business_date": self.business_date.isoformat(),
            "trigger": self.trigger.value, "priority": self.priority.name, "requested_by": self.requested_by,
            "attempt": self.attempt, "retry_of": self.retry_of, "status": self.status.value,
            "wait_reason": "; ".join(r.text for r in self.reasons) if self.reasons else None,
            "reasons": [r.to_dict() for r in self.reasons],
            "source_versions": self.snapshot, "newer_versions_available": drift,
            "staging": self.staging, "stage": self.stage, "progress": round(self.progress, 2),
            "error": self.error, "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }
