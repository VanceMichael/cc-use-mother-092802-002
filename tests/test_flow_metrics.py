"""派生指标：假期总量、日均、子项求和与可比增幅。"""

import unittest
from decimal import Decimal

from src.flow_caliber import DEFAULT_CALIBER_ID, DEFAULT_TREE, Caliber
from tests.flow_fixtures import (
    AT,
    DAYS,
    PERIOD,
    REF_DAYS,
    REF_END,
    REF_PERIOD,
    REF_START,
    SOURCES,
    fill_all_modes,
    make_batch,
    make_service,
    rec,
)


class MetricsTest(unittest.TestCase):
    def test_holiday_total_and_daily_average(self):
        service = make_service()
        fill_all_modes(service)
        metrics = service.metrics(PERIOD)
        self.assertEqual(metrics.days, 3)
        self.assertEqual(metrics.modes["rail"].total, Decimal(15300))
        self.assertEqual(metrics.modes["rail"].daily_average, Decimal(5100))
        self.assertEqual(metrics.modes["road"].total, Decimal(3160))
        self.assertEqual(metrics.holiday_total, Decimal(21550))
        self.assertEqual(metrics.holiday_daily_average, Decimal(21550) / Decimal(3))

    def test_road_total_falls_back_to_children(self):
        service = make_service()
        records = []
        for day in DAYS:
            records += [rec("road_commercial", day, 100), rec("road_private", day, 900)]
        result = service.submit_batch(make_batch("G1", SOURCES["road"], records), AT)
        self.assertEqual(result.status, "included")
        self.assertEqual(service.metrics(PERIOD).modes["road"].total, Decimal(3000))

    def test_comparable_growth_when_caliber_matches(self):
        service = make_service(compare_to=REF_PERIOD)
        service.register_period(REF_PERIOD, REF_START, REF_END)
        fill_all_modes(service, prefix="ref-", period=REF_PERIOD, days=REF_DAYS,
                       start=REF_START, end=REF_END)
        fill_all_modes(service, factor=2)
        growth = service.metrics(PERIOD).growth
        self.assertIsNotNone(growth)
        self.assertTrue(growth.comparable)
        self.assertEqual(growth.reference_period_id, REF_PERIOD)
        self.assertEqual(growth.rate, Decimal("100.0000"))

    def test_growth_not_comparable_across_caliber_versions(self):
        service = make_service(compare_to=REF_PERIOD)
        service.register_period(REF_PERIOD, REF_START, REF_END)
        service.register_caliber(Caliber(DEFAULT_CALIBER_ID, 2, DEFAULT_TREE))
        fill_all_modes(service, prefix="ref-", period=REF_PERIOD, days=REF_DAYS,
                       start=REF_START, end=REF_END)
        fill_all_modes(service, caliber_version=2)
        growth = service.metrics(PERIOD).growth
        self.assertIsNotNone(growth)
        self.assertFalse(growth.comparable)
        self.assertIsNone(growth.rate)
        self.assertIn("口径", growth.note)


if __name__ == "__main__":
    unittest.main()
