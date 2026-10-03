"""人工调整双人复核、发布不可变与勘误衔接、历史版本解释。"""

import unittest
from decimal import Decimal

from tests.flow_fixtures import (
    AT,
    PERIOD,
    SOURCES,
    fill_all_modes,
    make_service,
    simple_batch,
)


class AdjustmentTest(unittest.TestCase):
    def test_self_review_is_rejected(self):
        service = make_service()
        fill_all_modes(service)
        adjustment = service.propose_adjustment(
            PERIOD, "road", Decimal("50"), "抽样复核修正", "分析员甲", AT
        )
        with self.assertRaisesRegex(ValueError, "复核人不能与提交人相同"):
            service.review_adjustment(adjustment.adjustment_id, "分析员甲", True, AT)

    def test_approved_adjustment_enters_metrics(self):
        service = make_service()
        fill_all_modes(service)
        before = service.metrics(PERIOD).modes["road"].total
        adjustment = service.propose_adjustment(
            PERIOD, "road", Decimal("50"), "抽样复核修正", "分析员甲", AT
        )
        # 待复核的调整不参与汇总
        self.assertEqual(service.metrics(PERIOD).modes["road"].total, before)
        service.review_adjustment(adjustment.adjustment_id, "审核员乙", True, AT)
        self.assertEqual(service.metrics(PERIOD).modes["road"].total, before + Decimal(50))

    def test_rejected_adjustment_is_ignored(self):
        service = make_service()
        fill_all_modes(service)
        before = service.metrics(PERIOD).modes["road"].total
        adjustment = service.propose_adjustment(
            PERIOD, "road", Decimal("50"), "抽样复核修正", "分析员甲", AT
        )
        service.review_adjustment(adjustment.adjustment_id, "审核员乙", False, AT)
        self.assertEqual(service.metrics(PERIOD).modes["road"].total, before)


class PublishTest(unittest.TestCase):
    def test_published_version_is_immutable_and_errata_links_forward(self):
        service = make_service()
        fill_all_modes(service)
        v1 = service.publish(PERIOD, "首报", AT)
        total_v1 = v1.metrics.holiday_total
        service.submit_batch(
            simple_batch(
                "R2", SOURCES["rail"], "rail", (5000, 5100, 5300),
                revises="R1", reason="末日报送延迟补录",
            ),
            AT,
        )
        v2 = service.publish_errata(PERIOD, "铁路末日报送延迟补录", AT)
        self.assertEqual(v2.errata_of, v1.version_id)
        # 原版本保持原样，不原地覆盖
        stored_v1 = service.versions(PERIOD)[0]
        self.assertEqual(stored_v1.version_id, v1.version_id)
        self.assertEqual(stored_v1.metrics.holiday_total, total_v1)
        self.assertEqual(v2.metrics.holiday_total, total_v1 + Decimal(100))
        self.assertEqual(service.latest_version(PERIOD).version_id, v2.version_id)

    def test_errata_requires_existing_version(self):
        service = make_service()
        with self.assertRaisesRegex(ValueError, "无法勘误"):
            service.publish_errata(PERIOD, "测试", AT)


class ExplainTest(unittest.TestCase):
    def test_explain_lists_batches_pending_and_differences(self):
        service = make_service()
        fill_all_modes(service)
        # 制造一条待确认数据：编号相同内容不同的批次被隔离
        service.submit_batch(simple_batch("G1", SOURCES["road"], "road", (1, 2, 3)), AT)
        v1 = service.publish(PERIOD, "首报", AT)
        service.submit_batch(
            simple_batch(
                "R2", SOURCES["rail"], "rail", (5000, 5100, 5300),
                revises="R1", reason="末日报送延迟补录",
            ),
            AT,
        )
        v2 = service.publish_errata(PERIOD, "铁路补报", AT)

        first = service.explain(v1.version_id)
        self.assertIsNone(first.differs_from)
        self.assertIn("R1", [b["batch_id"] for b in first.batches_used])
        self.assertTrue(any("隔离" in item for item in first.pending))

        second = service.explain(v2.version_id)
        self.assertEqual(second.differs_from, v1.version_id)
        self.assertIn("R2", [b["batch_id"] for b in second.batches_used])
        text = "\n".join(second.differences)
        self.assertIn("rail", text)
        self.assertIn("假期总量", text)
        self.assertIn("末日报送延迟补录", text)
        self.assertEqual(second.errata_of, v1.version_id)

    def test_explain_unknown_version_raises(self):
        service = make_service()
        with self.assertRaisesRegex(ValueError, "不存在"):
            service.explain(f"{PERIOD}-v9")


if __name__ == "__main__":
    unittest.main()
