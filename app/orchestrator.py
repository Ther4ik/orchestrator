"""Ядро оркестратора: центральный диспетчер + Decision Engine.

Принципы:
* Весь доступ к состоянию идёт под одним RLock -> решения принимаются атомарно.
* Ресурсы (чтение источников, слот нагрузки) выдаются ПАКЕТОМ перед стартом или не выдаются вовсе
  (нет «захвати X и жди Y») -> взаимные блокировки (deadlock) невозможны (К-03).
* Ядро синхронное и детерминированное: исполнитель (runner) лишь вызывает complete/fail/report_progress.
  Поэтому ядро тестируется без asyncio и без FastAPI.
"""
import itertools
import threading
from dataclasses import dataclass
from datetime import date
from typing import Callable, Optional

from .catalog import PROCESSES, SOURCES, Weight
from .models import (ACTIVE, Decision, Priority, Reason, Source, SourceStatus, Status, Task, Trigger,
                     Verdict, now)


@dataclass
class Limits:
    """Квоты нагрузки (К-07). Числа условные для прототипа."""
    max_running: int = 5
    max_load: int = 8      # сумма весов: лёгкий=1, средний=2, тяжёлый=4
    max_heavy: int = 2


class Orchestrator:
    def __init__(self, limits: Optional[Limits] = None, on_start: Optional[Callable[[Task], None]] = None):
        self.limits = limits or Limits()
        self.on_start = on_start or (lambda task: None)   # хук для исполнителя
        self._lock = threading.RLock()
        self._seq = itertools.count(1)
        self.tasks: dict = {}
        self.sources = {sid: Source(sid, name) for sid, name in SOURCES.items()}
        self.results: dict = {}   # (process_id, date) -> опубликованный результат
        self.audit: list = []     # журнал для истории/таймлайна

    # ------------------------------------------------------------------ журнал
    def _log(self, event: str, task: Optional[Task] = None, text: str = ""):
        self.audit.append({"ts": now().isoformat(), "event": event,
                           "task_id": task.id if task else None,
                           "process_id": task.process_id if task else None, "text": text})

    # ------------------------------------------------------------------ публичный API
    def submit(self, process_id: str, business_date: date, trigger: Trigger = Trigger.MANUAL,
               requested_by: str = "system", simulate_failure_at: Optional[str] = None,
               attempt: int = 1, retry_of: Optional[str] = None) -> tuple:
        """Принять запрос на запуск. Возвращает (task, decision)."""
        if process_id not in PROCESSES:
            raise ValueError(f"Неизвестный процесс {process_id}")
        proc = PROCESSES[process_id]
        with self._lock:
            n = next(self._seq)
            priority = (Priority.EMERGENCY if proc.emergency
                        else Priority.SCHEDULED if trigger == Trigger.SCHEDULED else Priority.MANUAL)
            task = Task(id=f"T-{n:04d}", seq=n, process_id=process_id, business_date=business_date,
                        trigger=trigger, priority=priority, requested_by=requested_by,
                        attempt=attempt, retry_of=retry_of, simulate_failure_at=simulate_failure_at)
            self.tasks[task.id] = task
            self._log("submitted", task, f"{trigger.value}, {requested_by}, дата {business_date}")

            # К-01 (вариант В): идентичный активный запрос -> не плодим дубль, ссылаемся на существующий
            dup = next((t for t in self.tasks.values() if t.id != task.id and t.status in ACTIVE
                        and t.process_id == process_id and t.business_date == business_date), None)
            if dup:
                decision = Decision(Verdict.REJECT, [Reason(
                    "К-01", f"Запуск {process_id} за {business_date} уже {dup.status.value.lower()} ({dup.id})",
                    dup.id)])
                self._reject(task, decision)
                return task, decision

            decision = self.evaluate(task)
            if decision.verdict == Verdict.RUN:
                self._start(task)
            elif decision.verdict == Verdict.REJECT:
                self._reject(task, decision)
            else:
                task.reasons = decision.reasons
                self._log("queued", task, "; ".join(r.text for r in decision.reasons))
            return task, decision

    def cancel(self, task_id: str) -> Task:
        with self._lock:
            task = self._get(task_id)
            if task.status not in ACTIVE:
                raise ValueError(f"Задачу в статусе «{task.status.value}» отменить нельзя")
            was_running = task.status == Status.RUNNING
            self._finish(task, Status.CANCELLED, error="Отменено пользователем")
            if was_running:
                self._release(task)
                self._discard_staging(task)
            self._dispatch()
            return task

    def retry(self, task_id: str, requested_by: Optional[str] = None) -> tuple:
        """К-06: новый запуск с новым ID и историей попыток. Остатки прошлой попытки уже очищены."""
        with self._lock:
            old = self._get(task_id)
            if old.status not in (Status.FAILED, Status.CANCELLED):
                raise ValueError("Повторить можно только задачу в статусе «Ошибка» или «Отменено»")
            return self.submit(old.process_id, old.business_date, old.trigger,
                               requested_by or old.requested_by, attempt=old.attempt + 1, retry_of=old.id)

    # --- вызываются исполнителем ---
    def report_progress(self, task_id: str, stage: str, progress: float):
        with self._lock:
            task = self._get(task_id)
            if task.status == Status.RUNNING:
                task.stage, task.progress = stage, progress

    def complete(self, task_id: str):
        with self._lock:
            task = self._get(task_id)
            if task.status != Status.RUNNING:
                return
            # атомарная публикация из staging в «целевые таблицы»
            self.results[(task.process_id, task.business_date)] = {
                "task_id": task.id, "process_id": task.process_id,
                "business_date": task.business_date.isoformat(), "source_versions": dict(task.snapshot),
                "published_at": now().isoformat()}
            task.progress = 1.0
            self._finish(task, Status.SUCCESS)
            self._release(task)
            self._dispatch()

    def fail(self, task_id: str, error: str = "Сбой выполнения"):
        with self._lock:
            task = self._get(task_id)
            if task.status != Status.RUNNING:
                return
            self._finish(task, Status.FAILED, error=error)
            self._release(task)          # ресурсы освобождаются ВСЕГДА
            self._discard_staging(task)  # частичные результаты не публикуются
            self._dispatch()

    # --- источники данных ---
    def update_source(self, source_id: str, mode: str = "versioned") -> dict:
        """Поступили новые данные в источник.
        versioned  — создаётся новая версия, текущие расчёты продолжают читать свою (К-04);
        exclusive  — запись с эксклюзивным доступом: ждёт, пока читатели закончат (К-02)."""
        with self._lock:
            src = self._source(source_id)
            if mode == "versioned":
                self._bump(src)
                self._log("source_updated", None, f"{source_id} -> v{src.version} (versioned)")
                self._dispatch()
                return {"applied": True, "version": src.version}
            if mode == "exclusive":
                if src.readers:
                    src.pending_exclusive = True
                    self._log("source_update_pending", None,
                              f"{source_id}: запись ждёт завершения чтения {sorted(src.readers)}")
                    return {"applied": False, "waiting_for": sorted(src.readers), "version": src.version}
                self._bump(src)
                self._log("source_updated", None, f"{source_id} -> v{src.version} (exclusive)")
                return {"applied": True, "version": src.version}
            raise ValueError("mode: versioned | exclusive")

    def set_source_status(self, source_id: str, status: SourceStatus):
        """К-05: ручная настройка готовности источника. Переход в «Готов» запускает ожидающие задачи."""
        with self._lock:
            src = self._source(source_id)
            src.status = status
            self._log("source_status", None, f"{source_id}: {status.value}")
            self._dispatch()

    # --- чтение состояния ---
    def get(self, task_id: str) -> Task:
        with self._lock:
            return self._get(task_id)

    def current_versions(self) -> dict:
        return {sid: s.version for sid, s in self.sources.items()}

    def queue(self) -> list:
        with self._lock:
            return sorted((t for t in self.tasks.values() if t.status == Status.QUEUED), key=self._order)

    def load(self) -> dict:
        with self._lock:
            running = self._running()
            return {"running": len(running), "max_running": self.limits.max_running,
                    "load": sum(self._w(t) for t in running), "max_load": self.limits.max_load,
                    "heavy": sum(1 for t in running if self._w(t) == Weight.HEAVY.value),
                    "max_heavy": self.limits.max_heavy, "queued": len(self.queue())}

    # ================================================================== Decision Engine
    def evaluate(self, task: Task) -> Decision:
        """Единый конвейер проверок. Вердикт: запустить / в очередь (с причинами) / отклонить."""
        proc = PROCESSES[task.process_id]
        running = [t for t in self._running() if t.id != task.id]
        queue_reasons, reject_reasons = [], []

        # К-05: готовность источников
        for sid in proc.sources:
            src = self.sources[sid]
            if src.status == SourceStatus.ERROR:
                reject_reasons.append(Reason("К-05", f"Источник {sid} ({src.name}) в статусе «Ошибка» — запуск невозможен"))
            elif src.status != SourceStatus.READY:
                queue_reasons.append(Reason("К-05", f"Источник {sid} ({src.name}) не готов: {src.status.value}"))

        # К-01 / К-03: тот же расчёт (группа) за тот же период уже выполняется
        for t in running:
            other = PROCESSES[t.process_id]
            if other.group == proc.group and t.business_date == task.business_date:
                queue_reasons.append(Reason(
                    "К-01", f"Ожидает завершения {t.process_id} ({t.id}) — пересечение по периоду {task.business_date} и данным",
                    t.id))

        # К-02: источнику нужна эксклюзивная запись -> новых читателей не пускаем (иначе запись «голодает»)
        for sid in proc.sources:
            src = self.sources[sid]
            if src.pending_exclusive:
                queue_reasons.append(Reason(
                    "К-02", f"Источник {sid} ожидает эксклюзивного обновления — новое чтение отложено",
                    next(iter(sorted(src.readers)), None)))

        # К-07: квоты нагрузки
        w, lim = proc.weight.value, self.limits
        used = sum(self._w(t) for t in running)
        heavy = sum(1 for t in running if self._w(t) == Weight.HEAVY.value)
        if len(running) + 1 > lim.max_running:
            queue_reasons.append(Reason("К-07", f"Достигнут лимит параллельных задач ({lim.max_running})"))
        if used + w > lim.max_load:
            queue_reasons.append(Reason("К-07", f"Не хватает ресурсов: занято {used} из {lim.max_load}, нужно {w}"))
        if w == Weight.HEAVY.value and heavy + 1 > lim.max_heavy:
            queue_reasons.append(Reason("К-07", f"Лимит тяжёлых задач ({lim.max_heavy}) исчерпан"))

        if reject_reasons:
            return Decision(Verdict.REJECT, reject_reasons)
        if queue_reasons:
            return Decision(Verdict.QUEUE, queue_reasons)
        return Decision(Verdict.RUN)

    # ================================================================== внутренняя кухня
    def _dispatch(self):
        """Просмотр очереди по приоритету: стартуем всё, что теперь можно. Вызывается при любом
        освобождении ресурса или смене состояния источников. (Backfill: заблокированная задача
        не мешает стартовать следующим.)"""
        for task in sorted((t for t in self.tasks.values() if t.status == Status.QUEUED), key=self._order):
            decision = self.evaluate(task)
            if decision.verdict == Verdict.RUN:
                self._start(task)
            elif decision.verdict == Verdict.REJECT:
                self._reject(task, decision)
            else:
                task.reasons = decision.reasons

    def _start(self, task: Task):
        proc = PROCESSES[task.process_id]
        # пакетная выдача всех ресурсов: чтение источников + фиксация версий (snapshot, К-04)
        for sid in proc.sources:
            self.sources[sid].readers.add(task.id)
            task.snapshot[sid] = self.sources[sid].version
        task.staging = f"staging/{task.id}"   # у каждого запуска своя область записи -> К-03 (разные даты)
        task.status, task.reasons = Status.RUNNING, []
        task.started_at, task.progress = now(), 0.0
        self._log("started", task, f"версии источников: {task.snapshot}")
        self.on_start(task)

    def _release(self, task: Task):
        for src in self.sources.values():
            src.readers.discard(task.id)
            if src.pending_exclusive and not src.readers:   # дождались -> применяем отложенную запись
                src.pending_exclusive = False
                self._bump(src)
                self._log("source_updated", None, f"{src.id} -> v{src.version} (exclusive, отложенная запись)")

    def _discard_staging(self, task: Task):
        self._log("staging_discarded", task, f"{task.staging} очищена, частичные данные не опубликованы")
        task.staging = None

    def _finish(self, task: Task, status: Status, error: Optional[str] = None):
        task.status, task.error, task.finished_at = status, error, now()
        task.reasons = []
        self._log(status.name.lower(), task, error or "")

    def _reject(self, task: Task, decision: Decision):
        task.reasons = decision.reasons
        self._finish(task, Status.REJECTED, error="; ".join(r.text for r in decision.reasons))

    def _bump(self, src: Source):
        src.version += 1
        src.updated_at = now()

    def _running(self) -> list:
        return [t for t in self.tasks.values() if t.status == Status.RUNNING]

    @staticmethod
    def _w(task: Task) -> int:
        return PROCESSES[task.process_id].weight.value

    @staticmethod
    def _order(task: Task):
        return (task.priority.value, task.seq)

    def _get(self, task_id: str) -> Task:
        if task_id not in self.tasks:
            raise KeyError(task_id)
        return self.tasks[task_id]

    def _source(self, source_id: str) -> Source:
        if source_id not in self.sources:
            raise KeyError(source_id)
        return self.sources[source_id]
