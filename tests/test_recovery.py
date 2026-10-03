"""服务恢复：事件回放、快照一致、截止期提醒与待发布任务续跑。"""

from __future__ import annotations

import shutil
import unittest
from pathlib import Path

from src.mobility import AggregationService, QuarantineError, set_clock

from tests.mobility_support import DAYS, build_service


class RecoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc, self.path = build_service()

    def tearDown(self) -> None:
        shutil.rmtree(self.path, ignore_errors=True)
        set_clock(None)

    def _restart(self) -> AggregationService:
        return AggregationService(self.path)

    def test_rebuild_from_event_log_matches_live_state(self) -> None:
        self.svc.submit_batch("R-01", "rail-src", "rail",
                              {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 1900})
        self.svc.submit_batch("H-01", "road-src", "road-biz",
                              {DAYS[0]: 3500, DAYS[1]: 3400, DAYS[2]: 3600})
        self.svc.submit_batch("H-02", "road-src", "road-car",
                              {DAYS[0]: 15000, DAYS[1]: 14800, DAYS[2]: 15200})
        self.svc.create_publish_task("T-1", "编制假期出行快报",
                                     "2026-09-28T18:00:00", "分析员甲")
        # 不带快照，纯事件回放
        rebuilt = self._restart()
        self.assertEqual(rebuilt.metrics()["national"]["total"],
                         self.svc.metrics()["national"]["total"])
        self.assertEqual(rebuilt.state["tasks"]["T-1"].status, "open")

    def test_snapshot_replay_matches_and_survives_more_events(self) -> None:
        self.svc.submit_batch("R-01", "rail-src", "rail",
                              {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 1900})
        self.svc.checkpoint()
        # 快照之后再产生修订与发布事件
        self.svc.submit_batch(
            "R-01", "rail-src", "rail",
            {DAYS[0]: 1810, DAYS[1]: 1700, DAYS[2]: 1905},
            revision_of="R-01", revision_reason="终报修正",
        )
        self.svc.publish("v1", "甲", "快报")
        rebuilt = self._restart()
        self.assertEqual(rebuilt.metrics()["caliber:rail"]["total"], 5415.0)
        self.assertEqual([v["version"] for v in rebuilt.state["publications"]], ["v1"])
        self.assertEqual(rebuilt.state["_last_seq"], self.svc.state["_last_seq"])

    def test_resume_continues_open_tasks_reminders_quarantine_and_reviews(self) -> None:
        self.svc.submit_batch("R-01", "rail-src", "rail",
                              {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 1900})
        self.svc.create_publish_task("T-1", "编制快报", "2026-09-28T10:00:00", "分析员甲")
        self.svc.propose_adjustment("ADJ-1", "caliber", 5, "待复核补录",
                                    "分析员甲", caliber_id="rail", day=DAYS[0])
        try:
            self.svc.submit_batch("R-01", "rail-src", "rail",
                                  {DAYS[0]: 1801, DAYS[1]: 1700, DAYS[2]: 1900})
            self.fail("编号相同内容不同应隔离")
        except QuarantineError as ctx:
            quarantined_id = ctx.quarantined_id

        self.svc.checkpoint()
        rebuilt = self._restart()
        info = rebuilt.resume("2026-09-28T09:30:00")
        self.assertEqual([t["task_id"] for t in info["open_tasks"]], ["T-1"])
        self.assertIn("ADJ-1", info["pending_adjustments"])
        self.assertTrue(any(q["id"] == quarantined_id for q in info["quarantine"]))
        # 截止期提醒在恢复后继续（铁路口径已报齐，只剩待发布任务）
        kinds = {(r["kind"], r.get("task_id")) for r in info["reminders_due"]}
        self.assertIn(("publish_task", "T-1"), kinds)
        # 恢复后可继续完成未竟流程：复核与任务关闭
        rebuilt.review_adjustment("ADJ-1", "分析员乙", True, "恢复后复核")
        rebuilt.complete_task("T-1")
        again = self._restart()
        self.assertEqual(again.state["tasks"]["T-1"].status, "completed")
        self.assertEqual(again.state["adjustments"]["ADJ-1"].status, "approved")

    def test_reminders_dedup_and_refire_when_state_changes(self) -> None:
        # 铁路口径尚未报送；截止期临期
        fired = self.svc.fire_due_reminders("2026-09-28T09:00:00")
        caliber_reminders = [f for f in fired if f["kind"] == "deadline"]
        self.assertTrue(caliber_reminders)
        # 同一缺失状态不重复提醒
        self.assertEqual(
            self.svc.fire_due_reminders("2026-09-28T09:30:00"), [])
        # 报齐两天后仍缺一天：缺失集合变化，允许再次提醒
        self.svc.submit_batch("R-01", "rail-src", "rail",
                              {DAYS[0]: 1800, DAYS[1]: 1700})
        fired2 = self.svc.fire_due_reminders("2026-09-28T10:00:00")
        rail = [f for f in fired2 if f.get("caliber_id") == "rail"]
        self.assertEqual(len(rail), 1)
        self.assertEqual(rail[0]["missing_days"], [DAYS[2]])
        # 全部报齐后不再提醒
        self.svc.submit_batch("R-01", "rail-src", "rail",
                              {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 1900},
                              revision_of="R-01", revision_reason="补齐末日")
        self.assertEqual(
            [f for f in self.svc.fire_due_reminders("2026-09-28T11:00:00")
             if f.get("caliber_id") == "rail"],
            [])

    def test_overdue_flag(self) -> None:
        rem = self.svc.pending_reminders("2026-09-29T08:00:00")
        self.assertTrue(all(r["overdue"] for r in rem if r["kind"] == "deadline"))

    def test_torn_tail_after_crash_is_truncated_on_restart(self) -> None:
        self.svc.submit_batch("R-01", "rail-src", "rail",
                              {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 1900})
        last_good_seq = self.svc.state["_last_seq"]
        # 模拟崩溃：在日志末尾追加写了一半的事件
        log = Path(self.path) / "events.logl"
        with log.open("a", encoding="utf-8") as fh:
            fh.write('{"seq": ' + str(last_good_seq + 1) + ', "type": "batch_rec')
        # 重启应截掉半行并恢复到最后完整状态，且可继续追加
        rebooted = self._restart()
        self.assertEqual(rebooted.state["_last_seq"], last_good_seq)
        self.assertEqual(rebooted.metrics()["caliber:rail"]["total"], 5400.0)
        rebooted.create_publish_task("T-9", "崩溃后续跑任务",
                                     "2026-09-28T18:00:00", "分析员甲")
        again = self._restart()
        self.assertEqual(again.state["tasks"]["T-9"].status, "open")
        self.assertEqual(again.state["_last_seq"], last_good_seq + 1)


if __name__ == "__main__":
    unittest.main()
