"""派生指标：日均、假期总量、可比增幅，以及补报时的增量重算。"""

from __future__ import annotations

import shutil
import unittest

from src.mobility import set_clock
from src.mobility.models import Caliber, Mode

from tests.mobility_support import DAYS, build_service


def derived_event_keys(svc, after_seq: int) -> list[str]:
    return [
        ev["payload"]["key"]
        for ev in svc.store.iter_events(after_seq)
        if ev["type"] == "derived_computed"
    ]


class MetricsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc, self.path = build_service()

    def tearDown(self) -> None:
        shutil.rmtree(self.path, ignore_errors=True)
        set_clock(None)

    def _submit_all_but_water(self) -> None:
        self.svc.submit_batch("R-01", "rail-src", "rail",
                              {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 1900})
        self.svc.submit_batch("H-01", "road-src", "road-biz",
                              {DAYS[0]: 3500, DAYS[1]: 3400, DAYS[2]: 3600})
        self.svc.submit_batch("H-02", "road-src", "road-car",
                              {DAYS[0]: 15000, DAYS[1]: 14800, DAYS[2]: 15200})
        self.svc.submit_batch("H-03", "road-src", "road-total",
                              {DAYS[0]: 18500, DAYS[1]: 18200, DAYS[2]: 18800})
        self.svc.submit_batch("A-01", "air-src", "air",
                              {DAYS[0]: 160, DAYS[1]: 165, DAYS[2]: 175})

    def test_daily_average_total_and_comparable_growth(self) -> None:
        self._submit_all_but_water()
        rail = self.svc.metrics()["caliber:rail"]
        self.assertEqual(rail["total"], 5400.0)
        self.assertEqual(rail["daily_average"], 1800.0)
        # 上年同期三天 5000 万：5400/5000 - 1 = 8%
        self.assertAlmostEqual(rail["growth"], 0.08, places=6)

    def test_growth_uses_daily_average_when_holiday_length_differs(self) -> None:
        # 去年假期四天共 6400（日均 1600），今年三天口径总量 4800（日均 1600）→ 0%
        from src.mobility import Caliber as Cal
        self.svc.register_source("road2-src", "另一公路机构")
        self.svc.define_caliber(Cal(
            "road2-biz", Mode.ROAD, "营业性客运（另一辖区）", "road2-src",
            prior_year_total=6400, prior_year_days=4,
        ))
        self.svc.submit_batch("K-01", "road2-src", "road2-biz",
                              {DAYS[0]: 1600, DAYS[1]: 1600, DAYS[2]: 1600})
        self.assertAlmostEqual(
            self.svc.metrics()["caliber:road2-biz"]["growth"], 0.0, places=6)

    def test_growth_without_prior_baseline_is_none(self) -> None:
        # 无上年基数的口径增幅为 None，而不是报错或伪造增幅
        self.svc.define_caliber(Caliber("charter", Mode.AIR, "包机临时统计", "air-src"))
        self.svc.submit_batch("C9", "air-src", "charter", {DAYS[0]: 12})
        self.assertIsNone(self.svc.metrics()["caliber:charter"]["growth"])

    def test_national_total_sums_four_modes_and_keeps_pending(self) -> None:
        self._submit_all_but_water()
        national = self.svc.metrics()["national"]
        # 铁路 5400 + 公路 55500 + 民航 500 = 61400，水路待确认
        self.assertEqual(national["total"], 61400.0)
        self.assertEqual(national["daily_average"], round(61400 / 3, 4))
        self.assertIn("water", national["pending"])
        # 发布快照应列明待确认项
        snap = self.svc.publish("v1", "审核人", "快报")
        self.assertTrue(any(p["caliber_id"] == "water" for p in snap["pending"]))

    def test_mode_supplement_only_recomputes_affected_derivations(self) -> None:
        self._submit_all_but_water()
        seq_before = self.svc.state["_last_seq"]
        # 水路补报：只应重算 水路口径、水路方式、全国总量
        self.svc.submit_batch("W-01", "water-src", "water",
                              {DAYS[0]: 95, DAYS[1]: 100, DAYS[2]: 105})
        keys = derived_event_keys(self.svc, seq_before)
        self.assertEqual(keys, ["caliber:water", "mode:water", "national"])
        national = self.svc.metrics()["national"]
        self.assertEqual(national["total"], 61700.0)
        self.assertEqual(national["pending"], [])

    def test_no_change_submission_does_not_recompute(self) -> None:
        self._submit_all_but_water()
        seq_before = self.svc.state["_last_seq"]
        self.svc.submit_batch("R-01", "rail-src", "rail",
                              {DAYS[0]: 1800, DAYS[1]: 1700, DAYS[2]: 1900})
        # 判重事件可能产生，但不应触发任何派生指标重算
        self.assertEqual(derived_event_keys(self.svc, seq_before), [])

    def test_road_component_supplement_recomputes_aggregate_chain(self) -> None:
        # 合计口径由两个分项合成（来源不直接报合计）
        self.svc.submit_batch("H-01", "road-src", "road-biz",
                              {DAYS[0]: 3500, DAYS[1]: 3400, DAYS[2]: 3600})
        self.svc.submit_batch("H-02", "road-src", "road-car",
                              {DAYS[0]: 15000, DAYS[1]: 14800, DAYS[2]: 15200})
        self.assertEqual(self.svc.metrics()["caliber:road-total"]["total"], 55500.0)
        seq_before = self.svc.state["_last_seq"]
        # 营业性客运补报修订：营业性口径 → 公路合计 → 公路方式 → 全国
        self.svc.submit_batch(
            "H-01", "road-src", "road-biz",
            {DAYS[0]: 3510, DAYS[1]: 3400, DAYS[2]: 3600},
            revision_of="H-01", revision_reason="客运站终报修正",
        )
        keys = derived_event_keys(self.svc, seq_before)
        self.assertIn("caliber:road-biz", keys)
        self.assertIn("caliber:road-total", keys)
        self.assertIn("mode:road", keys)
        self.assertIn("national", keys)
        self.assertNotIn("caliber:rail", keys)
        self.assertEqual(self.svc.metrics()["caliber:road-total"]["total"], 55510.0)

    def test_component_revision_inconsistent_with_reported_total_is_rejected(self) -> None:
        self.svc.submit_batch("H-01", "road-src", "road-biz",
                              {DAYS[0]: 3500, DAYS[1]: 3400, DAYS[2]: 3600})
        self.svc.submit_batch("H-02", "road-src", "road-car",
                              {DAYS[0]: 15000, DAYS[1]: 14800, DAYS[2]: 15200})
        self.svc.submit_batch("H-03", "road-src", "road-total",
                              {DAYS[0]: 18500, DAYS[1]: 18200, DAYS[2]: 18800})
        # 分项改了但合计仍旧：包含关系不成立，修订被整笔拒绝
        with self.assertRaises(Exception):
            self.svc.submit_batch(
                "H-01", "road-src", "road-biz",
                {DAYS[0]: 3510, DAYS[1]: 3400, DAYS[2]: 3600},
                revision_of="H-01", revision_reason="客运站终报修正",
            )
        # 原批次仍有效，指标未被污染
        self.assertEqual(self.svc.metrics()["caliber:road-biz"]["total"], 10500.0)
        self.assertEqual(self.svc.get_batch("road-src/H-01#b1").status, "accepted")


if __name__ == "__main__":
    unittest.main()
