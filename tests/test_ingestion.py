"""批次归集：单位归一化、覆盖日期、去重、隔离、包含关系。"""

from __future__ import annotations

import shutil
import unittest

from src.mobility import (
    ContainmentError,
    QuarantineError,
    ValidationError,
    set_clock,
)

from tests.mobility_support import DAYS, build_service, standard_road_totals


class IngestionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc, self.path = build_service()

    def tearDown(self) -> None:
        shutil.rmtree(self.path, ignore_errors=True)
        set_clock(None)

    def test_unit_conversion_to_wan_renci(self) -> None:
        # 以人次报送：18000000 人次 = 1800 万人次
        self.svc.submit_batch("R-01", "rail-src", "rail", {
            DAYS[0]: 18_000_000, DAYS[1]: 17_000_000, DAYS[2]: 19_000_000,
        }, unit="人次")
        self.assertEqual(self.svc.metrics()["caliber:rail"]["total"], 5400.0)
        self.assertEqual(self.svc.get_batch("rail-src/R-01#b1").reported_unit, "人次")

    def test_unknown_unit_and_out_of_window_are_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.svc.submit_batch("R-01", "rail-src", "rail", {DAYS[0]: 1}, unit="人公里")
        with self.assertRaises(ValidationError):
            self.svc.submit_batch("R-02", "rail-src", "rail", {"2026-09-24": 1})

    def test_source_cannot_report_other_sources_caliber(self) -> None:
        with self.assertRaises(ValidationError):
            self.svc.submit_batch("X", "road-src", "rail", {DAYS[0]: 1})

    def test_duplicate_submission_is_not_double_counted(self) -> None:
        values = {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 1900}
        self.svc.submit_batch("R-01", "rail-src", "rail", values)
        dup = self.svc.submit_batch("R-01", "rail-src", "rail", dict(values))
        self.assertEqual(dup.status, "duplicate")
        self.assertEqual(dup.duplicate_of, "rail-src/R-01#b1")
        # 再以不同单位表达同一语义内容，仍判重
        dup2 = self.svc.submit_batch("R-01", "rail-src", "rail", {
            DAYS[0]: 18_000_000, DAYS[1]: 17_000_000, DAYS[2]: 19_000_000,
        }, unit="人次")
        self.assertEqual(dup2.status, "duplicate")
        self.assertEqual(self.svc.metrics()["caliber:rail"]["total"], 5400.0)

    def test_same_id_different_content_is_quarantined_and_excluded(self) -> None:
        self.svc.submit_batch("R-01", "rail-src", "rail", {DAYS[0]: 1800})
        with self.assertRaises(QuarantineError) as ctx:
            self.svc.submit_batch("R-01", "rail-src", "rail", {DAYS[0]: 1810})
        self.assertEqual(ctx.exception.display_id, "R-01")
        # 隔离内容不进入任何指标
        self.assertEqual(self.svc.metrics()["caliber:rail"]["total"], 1800.0)
        # 裁决接受：隔离内容接替旧批次
        self.svc.resolve_quarantine(ctx.exception.quarantined_id, "accept", "裁决人", "终报核实")
        self.assertEqual(self.svc.metrics()["caliber:rail"]["total"], 1810.0)

    def test_quarantine_reject_keeps_original(self) -> None:
        self.svc.submit_batch("R-09", "rail-src", "rail", {DAYS[0]: 1800})
        with self.assertRaises(QuarantineError) as ctx:
            self.svc.submit_batch("R-09", "rail-src", "rail", {DAYS[0]: 2000})
        self.svc.resolve_quarantine(ctx.exception.quarantined_id, "reject", "裁决人", "来源误报")
        self.assertEqual(self.svc.metrics()["caliber:rail"]["total"], 1800.0)
        self.assertEqual(
            self.svc.get_batch(ctx.exception.quarantined_id).status, "rejected")

    def test_explicit_revision_requires_reason_and_supersedes(self) -> None:
        self.svc.submit_batch("R-01", "rail-src", "rail", {DAYS[0]: 1800})
        with self.assertRaises(ValidationError):
            self.svc.submit_batch("R-01", "rail-src", "rail", {DAYS[0]: 1810},
                                  revision_of="R-01", revision_reason="")
        record = self.svc.submit_batch(
            "R-01", "rail-src", "rail", {DAYS[0]: 1810},
            revision_of="R-01", revision_reason="终报口径修正",
        )
        self.assertEqual(record.status, "accepted")
        self.assertEqual(self.svc.get_batch("rail-src/R-01#b1").status, "superseded")
        self.assertEqual(self.svc.metrics()["caliber:rail"]["total"], 1810.0)

    def test_road_containment_components_vs_aggregate(self) -> None:
        self.svc.submit_batch("H-01", "road-src", "road-biz",
                              {DAYS[0]: 3500, DAYS[1]: 3400, DAYS[2]: 3600})
        self.svc.submit_batch("H-02", "road-src", "road-car",
                              {DAYS[0]: 15000, DAYS[1]: 14800, DAYS[2]: 15200})
        # 偏差过大的合计被拒绝
        with self.assertRaises(ContainmentError):
            self.svc.submit_batch("H-03", "road-src", "road-total",
                                  {DAYS[0]: 9999, DAYS[1]: 9999, DAYS[2]: 9999})
        bad = self.svc.get_batch("road-src/H-03#b1")
        self.assertEqual(bad.status, "rejected")
        self.assertIn("包含关系", bad.resolution)
        # 被拒绝后可用原编号重新报送正确合计
        self.svc.submit_batch("H-03", "road-src", "road-total", standard_road_totals())
        self.assertEqual(self.svc.metrics()["caliber:road-total"]["total"], 55500.0)

    def test_different_batch_id_with_conflicting_day_is_rejected(self) -> None:
        self.svc.submit_batch("R-01", "rail-src", "rail",
                              {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 1900})
        # 另起编号覆盖同日不同值：拒绝，且总量不变
        with self.assertRaises(ValidationError):
            self.svc.submit_batch("R-02", "rail-src", "rail", {DAYS[0]: 1810})
        self.assertEqual(self.svc.metrics()["caliber:rail"]["total"], 5400.0)

    def test_aggregate_without_direct_report_uses_component_sum(self) -> None:
        self.svc.submit_batch("H-01", "road-src", "road-biz", {DAYS[0]: 3500})
        self.svc.submit_batch("H-02", "road-src", "road-car", {DAYS[0]: 15000})
        # 合计口径未直接报送：以营业性 + 小客车合成
        self.assertEqual(self.svc.metrics()["caliber:road-total"]["total"], 18500.0)


if __name__ == "__main__":
    unittest.main()
