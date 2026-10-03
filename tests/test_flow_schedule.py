"""截止期提醒、待发布任务与服务恢复。"""

import tempfile
import unittest
from datetime import date

from src.flow_service import FlowAggregationService
from src.flow_store import JsonFileStore
from tests.flow_fixtures import (
    AT,
    PERIOD,
    SOURCES,
    fill_all_modes,
    make_service,
    simple_batch,
)


class ScheduleTest(unittest.TestCase):
    def test_deadline_reminder_until_batch_arrives(self):
        service = make_service()
        service.plan_period(
            PERIOD,
            [{"source": SOURCES["rail"], "mode": "rail", "due": date(2026, 9, 28)}],
        )
        due = service.tick(date(2026, 9, 29))
        self.assertEqual(len(due), 1)
        self.assertIn("铁路", due[0].message)
        service.submit_batch(simple_batch("R1", SOURCES["rail"], "rail", (1, 2, 3)), AT)
        self.assertEqual(service.tick(date(2026, 9, 29)), [])

    def test_pending_publish_task_completes_on_publish(self):
        service = make_service()
        fill_all_modes(service)
        service.plan_period(PERIOD, [], publish_deadline=date(2026, 9, 29))
        self.assertEqual(len(service.recover()["pending_publish"]), 1)
        service.publish(PERIOD, "首报", AT)
        self.assertEqual(service.recover()["pending_publish"], [])

    def test_recovery_restores_state_from_file_store(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = JsonFileStore(temporary)
            first = make_service(store)
            fill_all_modes(first)
            first.plan_period(
                PERIOD,
                [{"source": "应急数据提供方", "mode": "water", "due": date(2026, 9, 28)}],
                publish_deadline=date(2026, 9, 29),
            )
            # 制造一条隔离记录，验证恢复后仍待确认
            first.submit_batch(simple_batch("R1", SOURCES["rail"], "rail", (9, 9, 9)), AT)
            expected_total = first.metrics(PERIOD).holiday_total

            recovered = FlowAggregationService(store)
            self.assertEqual(recovered.metrics(PERIOD).holiday_total, expected_total)
            tasks = recovered.recover()
            self.assertEqual(len(tasks["reminders"]), 1)
            self.assertEqual(tasks["reminders"][0].source, "应急数据提供方")
            self.assertEqual(len(tasks["pending_publish"]), 1)
            self.assertTrue(any("隔离" in item for item in recovered.pending_items(PERIOD)))
            # 恢复后继续受理报送，提醒随之完成
            recovered.submit_batch(
                simple_batch("Y1", "应急数据提供方", "water", (1, 1, 1)), AT
            )
            self.assertEqual(recovered.recover()["reminders"], [])


if __name__ == "__main__":
    unittest.main()
