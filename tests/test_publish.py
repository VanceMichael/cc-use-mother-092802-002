"""发布版本、勘误衔接、人工调整双人复核。"""

from __future__ import annotations

import shutil
import unittest

from src.mobility import (
    ContainmentError,
    NotFoundError,
    ValidationError,
    WorkflowError,
    set_clock,
)

from tests.mobility_support import DAYS, build_service


def submit_full_holiday(svc) -> None:
    svc.submit_batch("R-01", "rail-src", "rail",
                     {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 1900})
    svc.submit_batch("H-01", "road-src", "road-biz",
                     {DAYS[0]: 3500, DAYS[1]: 3400, DAYS[2]: 3600})
    svc.submit_batch("H-02", "road-src", "road-car",
                     {DAYS[0]: 15000, DAYS[1]: 14800, DAYS[2]: 15200})
    svc.submit_batch("H-03", "road-src", "road-total",
                     {DAYS[0]: 18500, DAYS[1]: 18200, DAYS[2]: 18800})
    svc.submit_batch("W-01", "water-src", "water",
                     {DAYS[0]: 95, DAYS[1]: 100, DAYS[2]: 105})
    svc.submit_batch("A-01", "air-src", "air",
                     {DAYS[0]: 160, DAYS[1]: 165, DAYS[2]: 175})


class PublishTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc, self.path = build_service()

    def tearDown(self) -> None:
        shutil.rmtree(self.path, ignore_errors=True)
        set_clock(None)

    def test_published_version_is_immutable_snapshot(self) -> None:
        submit_full_holiday(self.svc)
        v1 = self.svc.publish("2026-MID-AUTUMN-V1", "发布审核人员甲", "假期出行快报")
        self.assertEqual(v1["kind"], "published")
        self.assertIsNone(v1["supersedes"])
        total_v1 = v1["national"]["total"]
        # 铁路终报修订改变当前指标
        self.svc.submit_batch(
            "R-01", "rail-src", "rail",
            {DAYS[0]: 1810, DAYS[1]: 1700, DAYS[2]: 1905},
            revision_of="R-01", revision_reason="铁路终报口径修正",
        )
        # 历史版本快照不变：版本号不可重用
        with self.assertRaises(WorkflowError):
            self.svc.publish("2026-MID-AUTUMN-V1", "甲", "重复发布")
        snap_v1 = self.svc.explain_version("2026-MID-AUTUMN-V1")
        self.assertEqual(snap_v1["national_total"], total_v1)

    def test_corrigendum_chains_versions_and_explains_diff(self) -> None:
        submit_full_holiday(self.svc)
        self.svc.publish("v1", "甲", "快报")
        self.svc.submit_batch(
            "R-01", "rail-src", "rail",
            {DAYS[0]: 1810, DAYS[1]: 1700, DAYS[2]: 1905},
            revision_of="R-01", revision_reason="铁路终报口径修正",
        )
        v2 = self.svc.issue_corrigendum("v2", "乙", "铁路终报替换快报数，总量上调15万人次")
        self.assertEqual(v2["kind"], "corrigendum")
        self.assertEqual(v2["supersedes"], "v1")
        self.assertEqual(v2["corrigendum_reason"], "铁路终报替换快报数，总量上调15万人次")

        ex = self.svc.explain_version("v2")
        diff = ex["diff_against"]
        self.assertEqual(diff["prior_version"], "v1")
        nat_change = next(c for c in diff["metric_changes"] if c["scope"] == "national")
        self.assertEqual(nat_change["delta"], 15.0)
        actions = {(b["display_id"], b["action"]) for b in diff["batch_changes"]}
        self.assertIn(("R-01", "revision"), actions)
        self.assertIn(("R-01", "removed"), actions)
        # v1 无 diff 段，v2 有，链路清晰
        self.assertNotIn("diff_against", self.svc.explain_version("v1"))

    def test_corrigendum_requires_prior_and_reason(self) -> None:
        submit_full_holiday(self.svc)
        with self.assertRaises(WorkflowError):
            self.svc.issue_corrigendum("vX", "甲", "无初版先勘误")
        self.svc.publish("v1", "甲", "快报")
        with self.assertRaises(ValidationError):
            self.svc.issue_corrigendum("v2", "甲", "")

    def test_publish_blocks_when_containment_broken(self) -> None:
        # 分项与合计严丝合缝后，任何打破包含关系的调整在批准时即被拦截，
        # 因此不可能带着被破坏的包含关系进入发布
        submit_full_holiday(self.svc)
        self.svc.propose_adjustment(
            "ADJ-BAD", "caliber", 500, "营业性客运补录", "分析员甲",
            caliber_id="road-biz", day=DAYS[0],
        )
        with self.assertRaises(ContainmentError):
            self.svc.review_adjustment("ADJ-BAD", "分析员乙", True)
        # 调整保持待复核，发布不受污染，总量不变
        self.assertEqual(self.svc.state["adjustments"]["ADJ-BAD"].status, "pending")
        snap = self.svc.publish("v1", "甲", "快报")
        self.assertEqual(snap["national"]["total"], 61700.0)

    def test_explain_lists_pending_and_quarantined(self) -> None:
        # 仅铁路、公路营业性报齐，其余待确认
        self.svc.submit_batch("R-01", "rail-src", "rail",
                              {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 1900})
        self.svc.submit_batch("H-01", "road-src", "road-biz",
                              {DAYS[0]: 3500, DAYS[1]: 3400, DAYS[2]: 3600})
        v1 = self.svc.publish("v1", "甲", "阶段性快报")
        ex = self.svc.explain_version("v1")
        pending_ids = {p["caliber_id"] for p in ex["pending"]}
        self.assertIn("water", pending_ids)
        self.assertIn("air", pending_ids)
        # 小客车未报，公路合计应显示分项待确认
        self.assertTrue(any(p["caliber_id"] == "road-total" for p in ex["pending"]))
        # 批次构成可逐批追溯到来源与口径
        used = {(b["display_id"], b["source"], b["caliber"]) for b in ex["batches_used"]}
        self.assertIn(("R-01", "国家铁路局", "铁路客运量"), used)


class AdjustmentReviewTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc, self.path = build_service()
        submit_full_holiday(self.svc)

    def tearDown(self) -> None:
        shutil.rmtree(self.path, ignore_errors=True)
        set_clock(None)

    def test_adjustment_needs_second_person_and_takes_effect_after_approval(self) -> None:
        before = self.svc.metrics()["caliber:rail"]["total"]
        self.svc.propose_adjustment(
            "ADJ-1", "caliber", 12.5, "站点漏报补录", "分析员甲",
            caliber_id="rail", day=DAYS[2],
        )
        # 待复核期间不生效
        self.assertEqual(self.svc.metrics()["caliber:rail"]["total"], before)
        # 提交人不能复核自己的调整
        with self.assertRaises(WorkflowError):
            self.svc.review_adjustment("ADJ-1", "分析员甲", True)
        # 另一人驳回：仍不生效
        self.svc.review_adjustment("ADJ-1", "分析员乙", False, "凭证不足")
        self.assertEqual(self.svc.metrics()["caliber:rail"]["total"], before)
        # 重新发起并由另一人批准
        self.svc.propose_adjustment(
            "ADJ-2", "caliber", 12.5, "站点漏报补录（补凭证）", "分析员甲",
            caliber_id="rail", day=DAYS[2],
        )
        self.svc.review_adjustment("ADJ-2", "分析员乙", True, "凭证齐全")
        self.assertEqual(self.svc.metrics()["caliber:rail"]["total"], before + 12.5)
        # 已复核不可重复复核
        with self.assertRaises(WorkflowError):
            self.svc.review_adjustment("ADJ-2", "分析员丙", True)

    def test_approved_adjustment_appears_in_version_explanation(self) -> None:
        self.svc.publish("v1", "甲", "快报")
        self.svc.propose_adjustment(
            "ADJ-9", "national", -3.0, "剔除跨省重复统计", "分析员甲",
        )
        self.svc.review_adjustment("ADJ-9", "分析员乙", True, "确认重复")
        v2 = self.svc.issue_corrigendum("v2", "甲", "剔除跨省重复统计3万人次")
        ex = self.svc.explain_version("v2")
        self.assertIn("ADJ-9", {a["adjustment_id"] for a in ex["adjustments_applied"]})
        nat_change = next(c for c in ex["diff_against"]["metric_changes"]
                          if c["scope"] == "national")
        self.assertEqual(nat_change["delta"], -3.0)
        self.assertIn("ADJ-9", ex["diff_against"]["adjustments_added"])
        # 调整的提交人与复核人均留痕，可追责
        adj = next(a for a in ex["adjustments_applied"] if a["adjustment_id"] == "ADJ-9")
        self.assertEqual(adj["created_by"], "分析员甲")
        self.assertEqual(adj["reviewed_by"], "分析员乙")

    def test_unknown_adjustment_and_bad_target_rejected(self) -> None:
        with self.assertRaises(NotFoundError):
            self.svc.review_adjustment("nope", "乙", True)
        with self.assertRaises(ValidationError):
            self.svc.propose_adjustment("ADJ-X", "caliber", 1, "无口径", "甲",
                                        caliber_id="missing")
        with self.assertRaises(ValidationError):
            self.svc.propose_adjustment("ADJ-Y", "caliber", 1, "", "甲",
                                        caliber_id="rail")


if __name__ == "__main__":
    unittest.main()
