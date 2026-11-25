"""Тесты REST API (нужны fastapi и httpx; без них пропускаются)."""
import unittest

try:
    from fastapi.testclient import TestClient
    from app.main import app
    HAVE_API = True
except ImportError:      # pragma: no cover
    HAVE_API = False


@unittest.skipUnless(HAVE_API, "fastapi/httpx не установлены")
class Api(unittest.TestCase):
    def setUp(self):
        from app import main
        from app.orchestrator import Orchestrator
        from app.runner import SimulatedRunner
        main.orch = Orchestrator()                       # чистое состояние на каждый тест
        main.runner = SimulatedRunner(main.orch, 5.0)    # этапы долгие: тест успевает отменить задачу
        self._cm = TestClient(app)
        self.client = self._cm.__enter__()               # один event loop на тест -> фоновый исполнитель жив между запросами
        self.addCleanup(self._cm.__exit__, None, None, None)

    def post(self, **kw):
        body = {"process_id": "CR-01", "business_date": "2026-11-23", **kw}
        return self.client.post("/tasks", json=body)

    def test_create_and_get_task(self):
        r = self.post()
        self.assertEqual(r.status_code, 201)
        tid = r.json()["task"]["id"]
        self.assertEqual(self.client.get(f"/tasks/{tid}").status_code, 200)

    def test_processes_endpoint_lists_only_mvp_set(self):
        ids = {x["id"] for x in self.client.get("/processes").json()}
        self.assertEqual(ids, {"CR-01", "CR-07", "MR-01", "LR-03", "MR-08"})

    def test_sources_mark_only_demo_scenario_sources(self):
        marked = {x["id"]: x["demo"]["conflict"] for x in self.client.get("/sources").json() if x["demo"]}
        self.assertEqual(marked, {"SRC-001": "К-02", "SRC-006": "К-04", "SRC-008": "К-05"})

    def test_process_outside_mvp_set_is_404(self):
        self.assertEqual(self.post(process_id="CR-06").status_code, 404)

    def test_unknown_process_is_404(self):
        self.assertEqual(self.post(process_id="XX-99").status_code, 404)

    def test_bad_failure_stage_is_422(self):
        self.assertEqual(self.post(simulate_failure_at="nope").status_code, 422)

    def test_empty_requester_is_422(self):
        self.assertEqual(self.post(requested_by="").status_code, 422)

    def test_audit_limit_validated(self):
        self.assertEqual(self.client.get("/audit?limit=0").status_code, 422)
        self.assertEqual(self.client.get("/audit?limit=-5").status_code, 422)
        self.assertEqual(self.client.get("/audit?limit=10").status_code, 200)

    def test_cancel_and_retry_flow(self):
        tid = self.post().json()["task"]["id"]
        self.assertEqual(self.client.post(f"/tasks/{tid}/cancel").status_code, 200)
        self.assertEqual(self.client.post(f"/tasks/{tid}/cancel").status_code, 409)
        r = self.client.post(f"/tasks/{tid}/retry")
        self.assertEqual(r.status_code, 201)
        self.assertEqual(r.json()["task"]["attempt"], 2)

    def test_missing_task_is_404(self):
        self.assertEqual(self.client.get("/tasks/T-9999").status_code, 404)


if __name__ == "__main__":
    unittest.main()
