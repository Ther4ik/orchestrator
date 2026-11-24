"""Сценарии из описания конфликтов (5 обязательных + 2 опциональных). Запуск: python -m unittest -v"""
import unittest
from datetime import date

from app.models import SourceStatus, Status, Trigger
from app.orchestrator import Limits, Orchestrator

D = date(2026, 11, 23)
D2 = date(2026, 11, 22)
AUTO, MAN = Trigger.SCHEDULED, Trigger.MANUAL


class Scenarios(unittest.TestCase):
    def setUp(self):
        self.o = Orchestrator()

    def test_k01_manual_waits_for_scheduled(self):
        t1, d1 = self.o.submit("CR-01", D, AUTO, "scheduler")
        t2, d2 = self.o.submit("CR-07", D, MAN, "ivanov")
        self.assertEqual(t1.status, Status.RUNNING)
        self.assertEqual(t2.status, Status.QUEUED)
        self.assertEqual(d2.reasons[0].code, "К-01")
        self.assertEqual(d2.reasons[0].blocked_by, t1.id)
        self.o.complete(t1.id)                       # освободили -> второй стартует сам
        self.assertEqual(t2.status, Status.RUNNING)

    def test_k01_other_date_is_not_a_conflict(self):
        self.o.submit("CR-01", D, AUTO)
        t2, _ = self.o.submit("CR-07", D2, MAN)
        self.assertEqual(t2.status, Status.RUNNING)

    def test_k01_duplicate_request_rejected(self):
        t1, _ = self.o.submit("CR-01", D, AUTO)
        t2, d2 = self.o.submit("CR-01", D, MAN)
        self.assertEqual(t2.status, Status.REJECTED)
        self.assertEqual(d2.reasons[0].blocked_by, t1.id)

    def test_k01_cancel_queued(self):
        self.o.submit("CR-01", D, AUTO)
        t2, _ = self.o.submit("CR-07", D, MAN)
        self.o.cancel(t2.id)
        self.assertEqual(t2.status, Status.CANCELLED)

    def test_k02_parallel_reads_and_exclusive_write_waits(self):
        a, _ = self.o.submit("LR-02", D, AUTO)       # читает SRC-001
        b, _ = self.o.submit("CR-01", D, AUTO)       # тоже читает SRC-001
        self.assertEqual((a.status, b.status), (Status.RUNNING, Status.RUNNING))
        res = self.o.update_source("SRC-001", "exclusive")
        self.assertFalse(res["applied"])
        self.assertEqual(self.o.sources["SRC-001"].version, 1)
        c, dc = self.o.submit("CR-04", D, AUTO)      # новое чтение SRC-001 не пускаем перед записью
        self.assertEqual(c.status, Status.QUEUED)
        self.assertEqual(dc.reasons[0].code, "К-02")
        self.o.complete(a.id)
        self.assertEqual(self.o.sources["SRC-001"].version, 1)   # b ещё читает
        self.o.complete(b.id)
        self.assertEqual(self.o.sources["SRC-001"].version, 2)   # запись применилась
        self.assertEqual(c.status, Status.RUNNING)               # и читатель стартовал на новой версии
        self.assertEqual(c.snapshot["SRC-001"], 2)

    def test_k03_same_process_different_dates_parallel_isolated(self):
        a, _ = self.o.submit("CR-06", D2, MAN, "u1")
        b, _ = self.o.submit("CR-06", D, MAN, "u2")
        self.assertEqual((a.status, b.status), (Status.RUNNING, Status.RUNNING))
        self.assertNotEqual(a.staging, b.staging)

    def test_k03_different_processes_same_date_parallel(self):
        a, _ = self.o.submit("MR-01", D, AUTO)
        b, _ = self.o.submit("MR-02", D, AUTO)
        self.assertEqual((a.status, b.status), (Status.RUNNING, Status.RUNNING))

    def test_k03_batch_no_deadlock_everything_finishes(self):
        scheduled = ["CR-01", "CR-02", "CR-03", "MR-01", "MR-02", "MR-04", "MR-06", "LR-02", "LR-03", "LR-04"]
        manual = ["CR-06", "MR-07", "LR-07"]
        tasks = [self.o.submit(p, D, AUTO)[0] for p in scheduled] + \
                [self.o.submit(p, D, MAN)[0] for p in manual]
        self.assertTrue(any(t.status == Status.QUEUED for t in tasks))      # часть реально ждёт
        self.assertLessEqual(self.o.load()["running"], self.o.limits.max_running)
        for _ in range(100):                         # «исполнитель»: завершаем всё, что запущено
            running = [t for t in tasks if t.status == Status.RUNNING]
            if not running:
                break
            for t in running:
                self.o.complete(t.id)
        self.assertTrue(all(t.status == Status.SUCCESS for t in tasks))     # никто не завис
        self.assertEqual(self.o.load()["running"], 0)
        self.assertTrue(all(not s.readers for s in self.o.sources.values()))

    def test_k04_source_update_does_not_affect_running_task(self):
        t, _ = self.o.submit("MR-01", D, AUTO)
        self.assertEqual(t.snapshot["SRC-006"], 1)
        self.o.update_source("SRC-006", "versioned")            # v2 пришла в 18:02
        self.assertEqual(self.o.sources["SRC-006"].version, 2)
        self.assertEqual(t.snapshot["SRC-006"], 1)               # расчёт остался на v1
        self.assertEqual(t.to_dict(self.o.current_versions())["newer_versions_available"]["SRC-006"],
                         {"used": 1, "current": 2})
        self.o.complete(t.id)
        self.assertEqual(self.o.results[("MR-01", D)]["source_versions"]["SRC-006"], 1)

    def test_k05_waits_for_source_ready_then_autostarts(self):
        self.o.set_source_status("SRC-008", SourceStatus.UPDATING)
        t, d = self.o.submit("LR-03", D, AUTO)
        self.assertEqual(t.status, Status.QUEUED)
        self.assertEqual(d.reasons[0].code, "К-05")
        self.o.set_source_status("SRC-008", SourceStatus.READY)
        self.assertEqual(t.status, Status.RUNNING)

    def test_k05_source_error_rejects(self):
        self.o.set_source_status("SRC-008", SourceStatus.ERROR)
        t, d = self.o.submit("LR-03", D, AUTO)
        self.assertEqual(t.status, Status.REJECTED)

    def test_k06_failure_releases_resources_and_retry_works(self):
        t, _ = self.o.submit("CR-01", D, AUTO)
        self.o.fail(t.id, "обрыв связи")
        self.assertEqual(t.status, Status.FAILED)
        self.assertIsNone(t.staging)
        self.assertNotIn(("CR-01", D), self.o.results)           # частичный результат не опубликован
        self.assertTrue(all(not s.readers for s in self.o.sources.values()))
        t2, _ = self.o.retry(t.id)
        self.assertNotEqual(t2.id, t.id)
        self.assertEqual((t2.attempt, t2.retry_of, t2.status), (2, t.id, Status.RUNNING))

    def test_k06_failure_unblocks_waiting_task(self):
        a, _ = self.o.submit("CR-01", D, AUTO)
        b, _ = self.o.submit("CR-07", D, MAN)
        self.o.fail(a.id)
        self.assertEqual(b.status, Status.RUNNING)

    def test_k07_heavy_limit_and_priority(self):
        h1, _ = self.o.submit("MR-05", D, AUTO)
        h2, _ = self.o.submit("LR-06", D, AUTO)
        h3, d3 = self.o.submit("MR-08", D2, MAN)                 # экстренный, но лимит исчерпан
        self.assertEqual(h3.status, Status.QUEUED)
        self.assertTrue(any(r.code == "К-07" for r in d3.reasons))
        lite, _ = self.o.submit("CR-06", D, MAN)                 # обычный ручной встал в очередь за ним
        self.assertEqual(lite.status, Status.QUEUED)
        self.o.complete(h1.id)
        self.assertEqual(h3.status, Status.RUNNING)              # экстренный обогнал остальных
        self.assertEqual(self.o.load()["heavy"], 2)

    def test_limits_configurable(self):
        o = Orchestrator(Limits(max_running=1))
        a, _ = o.submit("MR-03", D, AUTO)
        b, _ = o.submit("LR-01", D, AUTO)
        self.assertEqual((a.status, b.status), (Status.RUNNING, Status.QUEUED))


if __name__ == "__main__":
    unittest.main()
