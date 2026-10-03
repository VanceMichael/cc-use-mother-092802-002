"""报送受理：重复报送去重、编号冲突隔离、单位与包含关系校验、修订。"""

import unittest
from decimal import Decimal

from tests.flow_fixtures import (
    AT,
    DAYS,
    PERIOD,
    SOURCES,
    fill_all_modes,
    make_batch,
    make_service,
    rec,
    simple_batch,
)


class IngestTest(unittest.TestCase):
    def test_duplicate_resubmission_is_not_counted_twice(self):
        service = make_service()
        first = service.submit_batch(
            simple_batch("R1", SOURCES["rail"], "rail", (100, 110, 120)), AT
        )
        self.assertEqual(first.status, "included")
        again = service.submit_batch(
            simple_batch("R1", SOURCES["rail"], "rail", (100, 110, 120)), AT
        )
        self.assertEqual(again.status, "duplicate")
        self.assertEqual(service.metrics(PERIOD).modes["rail"].total, Decimal(330))

    def test_same_id_different_content_is_quarantined_then_revised(self):
        service = make_service()
        service.submit_batch(simple_batch("R1", SOURCES["rail"], "rail", (100, 110, 120)), AT)
        conflict = service.submit_batch(
            simple_batch("R1", SOURCES["rail"], "rail", (100, 110, 999)), AT
        )
        self.assertEqual(conflict.status, "quarantined")
        self.assertIn("内容不同", conflict.issues[0])
        # 隔离期间汇总仍采用原批次
        self.assertEqual(service.metrics(PERIOD).modes["rail"].total, Decimal(330))
        resolved = service.resolve_quarantine(
            conflict.quarantine_id, "accept_revision", "分析员甲", "末日报送延迟补录", AT
        )
        self.assertEqual(resolved.status, "revised")
        self.assertEqual(service.metrics(PERIOD).modes["rail"].total, Decimal(1209))

    def test_quarantined_batch_can_be_rejected(self):
        service = make_service()
        service.submit_batch(simple_batch("R1", SOURCES["rail"], "rail", (100, 110, 120)), AT)
        conflict = service.submit_batch(
            simple_batch("R1", SOURCES["rail"], "rail", (100, 110, 999)), AT
        )
        resolved = service.resolve_quarantine(conflict.quarantine_id, "reject", "分析员甲", at=AT)
        self.assertEqual(resolved.status, "rejected")
        self.assertEqual(service.metrics(PERIOD).modes["rail"].total, Decimal(330))

    def test_person_trips_unit_is_converted(self):
        service = make_service()
        batch = make_batch(
            "R1", SOURCES["rail"], [rec("rail", d, 1000000, "人次") for d in DAYS]
        )
        result = service.submit_batch(batch, AT)
        self.assertEqual(result.status, "included")
        # 每天1000000人次=100万人次，三天合计300万人次
        self.assertEqual(service.metrics(PERIOD).modes["rail"].total, Decimal(300))

    def test_people_unit_is_rejected(self):
        service = make_service()
        batch = make_batch("R1", SOURCES["rail"], [rec("rail", d, 100, "万人") for d in DAYS])
        result = service.submit_batch(batch, AT)
        self.assertEqual(result.status, "quarantined")
        self.assertTrue(any("人数" in issue for issue in result.issues))

    def test_road_children_must_sum_to_parent(self):
        service = make_service()
        records = []
        for day in DAYS:
            records += [
                rec("road", day, 1000),
                rec("road_commercial", day, 100),
                rec("road_private", day, 800),
            ]
        result = service.submit_batch(make_batch("G1", SOURCES["road"], records), AT)
        self.assertEqual(result.status, "quarantined")
        self.assertTrue(any("超出容差" in issue for issue in result.issues))

    def test_incomplete_children_are_flagged(self):
        service = make_service()
        records = []
        for day in DAYS:
            records += [rec("road", day, 1000), rec("road_commercial", day, 100)]
        result = service.submit_batch(make_batch("G1", SOURCES["road"], records), AT)
        self.assertEqual(result.status, "quarantined")
        self.assertTrue(any("缺少子项" in issue for issue in result.issues))

    def test_unknown_mode_is_rejected(self):
        service = make_service()
        batch = make_batch("X1", SOURCES["rail"], [rec("pipeline", d, 1) for d in DAYS])
        result = service.submit_batch(batch, AT)
        self.assertEqual(result.status, "quarantined")
        self.assertTrue(any("未知分项" in issue for issue in result.issues))

    def test_revision_requires_reason(self):
        service = make_service()
        service.submit_batch(simple_batch("R1", SOURCES["rail"], "rail", (100, 110, 120)), AT)
        result = service.submit_batch(
            simple_batch("R2", SOURCES["rail"], "rail", (100, 110, 130), revises="R1"), AT
        )
        self.assertEqual(result.status, "quarantined")
        self.assertTrue(any("修订原因" in issue for issue in result.issues))

    def test_late_revision_recomputes_only_affected_modes(self):
        service = make_service()
        fill_all_modes(service)
        log_before = len(service.recompute_log())
        result = service.submit_batch(
            simple_batch(
                "R2", SOURCES["rail"], "rail", (5000, 5100, 5300),
                revises="R1", reason="末日报送延迟补录",
            ),
            AT,
        )
        self.assertEqual(result.status, "revised")
        entries = service.recompute_log()[log_before:]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["modes"], ["rail"])
        metrics = service.metrics(PERIOD)
        self.assertEqual(metrics.modes["rail"].total, Decimal(15400))
        self.assertEqual(metrics.modes["road"].total, Decimal(3160))


if __name__ == "__main__":
    unittest.main()
