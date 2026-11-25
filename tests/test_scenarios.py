"""Сценарии из описания конфликтов К-01…К-07 на пяти процессах MVP:
CR-01 (плановый ECL), CR-07 (ручной пересчёт ECL), MR-01 (VaR), LR-03 (Liquidity Gap),
MR-08 (экстренный стресс-тест, тяжёлый). Запуск: python -m unittest -v"""
import unittest
from datetime import date

from app.catalog import PROCESSES, SOURCES, STAGES, Weight
from app.models import SourceStatus, Status, Trigger
from app.orchestrator import Limits, Orchestrator

D = date(2026, 11, 23)
D2 = date(2026, 11, 22)
D3 = date(2026, 11, 21)
AUTO, MAN = Trigger.SCHEDULED, Trigger.MANUAL


class Catalog(unittest.TestCase):
    def test_exactly_five_mvp_processes(self):
        self.assertEqual(set(PROCESSES), {"CR-01", "CR-07", "MR-01", "LR-03", "MR-08"})

    def test_every_source_is_used_by_some_process(self):
        used = {s for p in PROCESSES.values() for s in p.sources}
        self.assertEqual(used, set(SOURCES))

    def test_weights_and_groups(self):
        self.assertEqual(PROCESSES["MR-08"].weight, Weight.HEAVY)
        self.assertTrue(PROCESSES["MR-08"].emergency)
        self.assertEqual(PROCESSES["CR-01"].group, PROCESSES["CR-07"].group)      # К-01: один расчёт
        self.assertNotEqual(PROCESSES["MR-01"].group, PROCESSES["MR-08"].group)   # разные расчёты


class Scenarios(unittest.TestCase):
    def setUp(self):
        self.o = Orchestrator()

    # ---------------------------------------------------------------- К-01
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

    def test_k01_different_calculations_run_in_parallel(self):
        a, _ = self.o.submit("MR-01", D, AUTO)       # VaR
        b, _ = self.o.submit("MR-08", D, MAN)        # стресс-тест: другой расчёт, параллельно можно
        self.assertEqual((a.status, b.status), (Status.RUNNING, Status.RUNNING))

    # ---------------------------------------------------------------- К-02
    def test_k02_parallel_reads_and_exclusive_write_waits(self):
        a, _ = self.o.submit("CR-01", D, AUTO)       # читает SRC-001
        b, _ = self.o.submit("LR-03", D, AUTO)       # тоже читает SRC-001
        self.assertEqual((a.status, b.status), (Status.RUNNING, Status.RUNNING))
        res = self.o.update_source("SRC-001", "exclusive")
        self.assertFalse(res["applied"])
        self.assertEqual(self.o.sources["SRC-001"].version, 1)
        c, dc = self.o.submit("CR-01", D2, AUTO)     # новое чтение SRC-001 не пускаем перед записью
        self.assertEqual(c.status, Status.QUEUED)
        self.assertTrue(any(r.code == "К-02" for r in dc.reasons))
        self.o.complete(a.id)
        self.assertEqual(self.o.sources["SRC-001"].version, 1)   # LR-03 ещё читает
        self.o.complete(b.id)
        self.assertEqual(self.o.sources["SRC-001"].version, 2)   # запись применилась
        self.assertEqual(c.status, Status.RUNNING)               # и читатель стартовал на новой версии
        self.assertEqual(c.snapshot["SRC-001"], 2)

    def test_k02_exclusive_update_applies_immediately_when_no_readers(self):
        res = self.o.update_source("SRC-001", "exclusive")
        self.assertTrue(res["applied"])
        self.assertEqual(self.o.sources["SRC-001"].version, 2)

    # ---------------------------------------------------------------- К-03
    def test_k03_same_process_different_dates_parallel_isolated(self):
        a, _ = self.o.submit("CR-01", D2, MAN, "u1")
        b, _ = self.o.submit("CR-01", D, MAN, "u2")
        self.assertEqual((a.status, b.status), (Status.RUNNING, Status.RUNNING))
        self.assertNotEqual(a.staging, b.staging)

    def test_k03_different_processes_same_date_parallel(self):
        a, _ = self.o.submit("CR-01", D, AUTO)
        b, _ = self.o.submit("MR-01", D, AUTO)       # оба читают SRC-003 — чтение совместимо
        self.assertEqual((a.status, b.status), (Status.RUNNING, Status.RUNNING))
        self.assertNotEqual(a.staging, b.staging)

    def test_k03_batch_no_deadlock_everything_finishes(self):
        tasks = []
        for day in (D, D2, D3):
            tasks += [self.o.submit(p, day, AUTO)[0] for p in ("CR-01", "MR-01", "LR-03")]
            tasks += [self.o.submit(p, day, MAN)[0] for p in ("CR-07", "MR-08")]
        self.assertTrue(any(t.status == Status.QUEUED for t in tasks))      # часть реально ждёт
        for _ in range(100):                          # «исполнитель»: завершаем всё, что запущено
            self.assertLessEqual(self.o.load()["running"], self.o.limits.max_running)
            running = [t for t in tasks if t.status == Status.RUNNING]
            if not running:
                break
            for t in running:
                self.o.complete(t.id)
        self.assertTrue(all(t.status == Status.SUCCESS for t in tasks))     # никто не завис, дублей нет
        self.assertEqual(self.o.load()["running"], 0)
        self.assertTrue(all(not s.readers for s in self.o.sources.values()))

    # ---------------------------------------------------------------- К-04
    def test_k04_source_update_does_not_affect_running_task(self):
        t, _ = self.o.submit("MR-01", D, AUTO)
        self.assertEqual(t.snapshot["SRC-006"], 1)
        self.o.update_source("SRC-006", "versioned")            # v2 пришла во время расчёта
        self.assertEqual(self.o.sources["SRC-006"].version, 2)
        self.assertEqual(t.snapshot["SRC-006"], 1)               # расчёт остался на v1
        self.assertEqual(t.to_dict(self.o.current_versions())["newer_versions_available"]["SRC-006"],
                         {"used": 1, "current": 2})
        self.o.complete(t.id)
        self.assertEqual(self.o.results[("MR-01", D)]["source_versions"]["SRC-006"], 1)

    # ---------------------------------------------------------------- К-05
    def test_k05_waits_for_source_ready_then_autostarts(self):
        self.o.set_source_status("SRC-008", SourceStatus.UPDATING)
        t, d = self.o.submit("LR-03", D, AUTO)
        self.assertEqual(t.status, Status.QUEUED)
        self.assertEqual(d.reasons[0].code, "К-05")
        self.o.set_source_status("SRC-008", SourceStatus.READY)
        self.assertEqual(t.status, Status.RUNNING)

    def test_k05_source_error_rejects(self):
        self.o.set_source_status("SRC-008", SourceStatus.ERROR)
        t, _ = self.o.submit("LR-03", D, AUTO)
        self.assertEqual(t.status, Status.REJECTED)

    def test_k05_unrelated_source_status_does_not_block(self):
        self.o.set_source_status("SRC-008", SourceStatus.UPDATING)   # LR-03 нужен, а CR-01 — нет
        t, _ = self.o.submit("CR-01", D, AUTO)
        self.assertEqual(t.status, Status.RUNNING)

    # ---------------------------------------------------------------- К-06
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

    def test_k06_cannot_retry_running_task(self):
        t, _ = self.o.submit("CR-01", D, AUTO)
        with self.assertRaises(ValueError):
            self.o.retry(t.id)

    # ---------------------------------------------------------------- К-07
    def test_k07_heavy_limit_and_priority(self):
        h1, _ = self.o.submit("MR-08", D, MAN)
        h2, _ = self.o.submit("MR-08", D2, MAN)
        h3, d3 = self.o.submit("MR-08", D3, MAN)                 # третий тяжёлый
        self.assertEqual((h1.status, h2.status, h3.status), (Status.RUNNING, Status.RUNNING, Status.QUEUED))
        self.assertEqual({r.code for r in d3.reasons}, {"К-07"})
        self.assertEqual(len(d3.reasons), 2)                     # и нагрузка 8/8, и лимит тяжёлых
        self.assertEqual(self.o.load()["load"], 8)
        cr7, _ = self.o.submit("CR-07", D, MAN)                  # даже средний не помещается при 8/8
        self.assertEqual(cr7.status, Status.QUEUED)
        self.o.complete(h1.id)
        self.assertEqual(h3.status, Status.RUNNING)              # экстренный обогнал ручной CR-07
        self.assertEqual(cr7.status, Status.QUEUED)
        self.assertEqual(self.o.load()["heavy"], 2)
        self.o.complete(h2.id)
        self.assertEqual(cr7.status, Status.RUNNING)             # дождался своей очереди

    def test_k07_scheduled_load_blocks_emergency_until_resources_free(self):
        a, _ = self.o.submit("CR-01", D, AUTO)
        b, _ = self.o.submit("MR-01", D, AUTO)
        c, _ = self.o.submit("LR-03", D, AUTO)                   # занято 6 из 8
        h, dh = self.o.submit("MR-08", D, MAN)                   # 6 + 4 > 8
        self.assertEqual(h.status, Status.QUEUED)
        self.assertEqual(dh.reasons[0].code, "К-07")
        self.o.complete(a.id)                                    # освободилось 2 -> 4 + 4 = 8
        self.assertEqual(h.status, Status.RUNNING)

    def test_limits_configurable(self):
        o = Orchestrator(Limits(max_running=1))
        a, _ = o.submit("MR-01", D, AUTO)
        b, _ = o.submit("LR-03", D, AUTO)
        self.assertEqual((a.status, b.status), (Status.RUNNING, Status.QUEUED))

    # ---------------------------------------------------------------- ядро: валидация и устойчивость
    def test_unknown_process_rejected(self):
        with self.assertRaises(ValueError):
            self.o.submit("CR-06", D, MAN)                       # вне набора MVP

    def test_unknown_failure_stage_rejected_by_core(self):
        with self.assertRaises(ValueError):
            self.o.submit("CR-01", D, AUTO, simulate_failure_at="nope")
        self.assertEqual(len(self.o.tasks), 0)                   # задача не создана

    def test_all_stages_are_valid_failure_points(self):
        for i, stage in enumerate(STAGES):
            t, _ = Orchestrator().submit("CR-01", date(2026, 11, 1 + i), AUTO, simulate_failure_at=stage)
            self.assertEqual(t.simulate_failure_at, stage)

    def test_runner_hook_failure_does_not_leak_resources(self):
        def boom(task):
            raise RuntimeError("нет event loop")
        o = Orchestrator(on_start=boom)
        t, _ = o.submit("CR-01", D, AUTO)
        self.assertEqual(t.status, Status.FAILED)
        self.assertIn("нет event loop", t.error)
        self.assertTrue(all(not s.readers for s in o.sources.values()))
        self.assertEqual(o.load()["running"], 0)


if __name__ == "__main__":
    unittest.main()
