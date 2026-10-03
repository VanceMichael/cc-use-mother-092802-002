"""端到端叙事：从分方式报送到勘误发布与跨重启的历史解释。"""

from __future__ import annotations

import shutil
import unittest

from src.mobility import AggregationService, set_clock

from tests.mobility_support import DAYS, build_service


class EndToEndStoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc, self.path = build_service()
        set_clock(lambda: "2026-09-28T08:00:00")

    def tearDown(self) -> None:
        shutil.rmtree(self.path, ignore_errors=True)
        set_clock(None)

    def test_full_lifecycle(self) -> None:
        svc = self.svc

        # —— 假期次日：仅铁路、公路到齐，先出快报（水路、民航待确认）——
        svc.submit_batch("R-01", "rail-src", "rail",
                         {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 1900})
        svc.submit_batch("H-01", "road-src", "road-biz",
                         {DAYS[0]: 3600, DAYS[1]: 3500, DAYS[2]: 3700})
        svc.submit_batch("H-02", "road-src", "road-car",
                         {DAYS[0]: 15600, DAYS[1]: 15400, DAYS[2]: 15800})
        # 公路合计不直接报送，由营业性 + 小客车合成 = 57600
        svc.create_publish_task("T-1", "发布假期出行快报", "2026-09-28T18:00:00", "分析员甲")
        v1 = svc.publish("bulletin-v1", "发布审核人员甲", "中秋假期出行快报（首日口径）")
        self.assertEqual(v1["national"]["total"], 5400.0 + 57600.0)
        pending = {p["caliber_id"] for p in v1["pending"]}
        self.assertEqual(pending, {"water", "air"})

        # —— 水路、民航补报：只重算各自口径/方式/全国，公路铁路不动 ——
        seq = svc.state["_last_seq"]
        svc.submit_batch("W-01", "water-src", "water",
                         {DAYS[0]: 102.5, DAYS[1]: 105.0, DAYS[2]: 108.6})
        svc.submit_batch("A-01", "air-src", "air",
                         {DAYS[0]: 200, DAYS[1]: 200, DAYS[2]: 200})
        recomputed = [
            ev["payload"]["key"] for ev in svc.store.iter_events(seq)
            if ev["type"] == "derived_computed"
        ]
        self.assertEqual(recomputed.count("caliber:rail"), 0)
        self.assertEqual(recomputed.count("caliber:road-total"), 0)
        self.assertIn("caliber:water", recomputed)
        self.assertIn("caliber:air", recomputed)

        # 5400 铁路 + 57600 公路 + 316.1 水路 + 600 民航 = 63916.1
        self.assertEqual(svc.metrics()["national"]["total"], 63916.1)

        # —— 水路以"人次"单位补正同一内容 → 判重，不重复累计 ——
        dup = svc.submit_batch("W-01", "water-src", "water",
                               {DAYS[0]: 1_025_000, DAYS[1]: 1_050_000, DAYS[2]: 1_086_000},
                               unit="人次")
        self.assertEqual(dup.status, "duplicate")
        self.assertEqual(svc.metrics()["caliber:water"]["total"], 316.1)

        # —— 铁路终报修订（迟到数据）——
        # 同内容再次报送仍判重
        svc.submit_batch(
            "R-01", "rail-src", "rail",
            {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 1900},
        )
        # 终报：末日 +500
        svc.submit_batch(
            "R-01", "rail-src", "rail",
            {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 2400},
            revision_of="R-01", revision_reason="铁路终报：末日临客加开修正500万人次",
        )

        # —— 人工调整：跨省重复统计剔除，必须双人复核 ——
        with self.assertRaises(Exception):
            # 提交人不能复核自己的调整
            svc.propose_adjustment("ADJ-2", "national", -100.0,
                                   "跨省接驳重复统计剔除", "分析员甲")
            svc.review_adjustment("ADJ-2", "分析员甲", True)
        # 由另一名人员复核通过
        svc.review_adjustment("ADJ-2", "分析员乙", True, "比对联网售票记录确认")

        # —— 勘误发布，不覆盖 v1 ——
        v2 = svc.issue_corrigendum(
            "bulletin-v2", "发布审核人员乙",
            "水路、民航补报并入；铁路终报修正；剔除跨省重复统计100万人次",
            title="中秋假期出行公报（终报口径）",
        )
        # 63916.1 - 快报铁路5400 + 终报铁路5900 - 100 重复剔除 = 64316.1
        self.assertEqual(v2["national"]["total"], 64316.1)
        self.assertEqual(v2["pending"], [])
        self.assertEqual(v2["supersedes"], "bulletin-v1")

        # —— 任一历史版本可解释 ——
        ex1 = svc.explain_version("bulletin-v1")
        self.assertEqual(ex1["national_total"], 63000.0)
        self.assertEqual(len(ex1["batches_used"]), 3)  # 铁路1批 + 公路2批
        self.assertNotIn("diff_against", ex1)

        ex2 = svc.explain_version("bulletin-v2")
        diff = ex2["diff_against"]
        scopes = {c["scope"]: c["delta"] for c in diff["metric_changes"]}
        self.assertAlmostEqual(scopes["national"], 64316.1 - 63000.0, places=4)
        self.assertEqual(scopes["mode:rail"], 500.0)
        self.assertEqual(scopes["mode:water"], 316.1)
        self.assertEqual(scopes["mode:air"], 600.0)
        # 批次级解释：水路/民航新增，铁路修订
        actions = {(b["display_id"], b["action"]) for b in diff["batch_changes"]}
        self.assertIn(("W-01", "added"), actions)
        self.assertIn(("A-01", "added"), actions)
        self.assertIn(("R-01", "revision"), actions)
        self.assertIn(("R-01", "removed"), actions)
        self.assertEqual(diff["adjustments_added"], ["ADJ-2"])

        # —— 重启后：版本、指标、任务、提醒全部可恢复并继续 ——
        svc.checkpoint()
        svc.complete_task("T-1")
        rebooted = AggregationService(self.path)
        info = rebooted.resume("2026-09-28T17:00:00")
        self.assertEqual(info["publications"], ["bulletin-v1", "bulletin-v2"])
        self.assertEqual(rebooted.metrics()["national"]["total"], 64316.1)
        self.assertEqual(info["open_tasks"], [])  # 已在重启前完成
        # 历史解释能力在重启后保持一致
        self.assertEqual(
            rebooted.explain_version("bulletin-v2")["national_total"], 64316.1)


if __name__ == "__main__":
    unittest.main()
