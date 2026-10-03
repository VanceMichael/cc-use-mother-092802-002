"""测试支撑：构建标准四方式归集环境。"""

from __future__ import annotations

import tempfile
from pathlib import Path

from src.mobility import AggregationService, Caliber, Mode, set_clock

DAYS = ("2026-09-25", "2026-09-26", "2026-09-27")


def build_service(tmp: str | Path | None = None) -> tuple[AggregationService, str]:
    set_clock(lambda: "2026-09-28T08:00:00")
    path = tmp or tempfile.mkdtemp(prefix="mobility-test-")
    svc = AggregationService(path)
    svc.open_holiday("2026年中秋假期", DAYS[0], DAYS[-1])
    svc.register_source("rail-src", "国家铁路局")
    svc.register_source("road-src", "公路数据提供方")
    svc.register_source("water-src", "水路数据提供方")
    svc.register_source("air-src", "民航数据提供方")
    svc.define_caliber(Caliber(
        "rail", Mode.RAIL, "铁路客运量", "rail-src",
        prior_year_total=5000, prior_year_days=3,
        deadline="2026-09-28T12:00:00",
    ))
    svc.define_caliber(Caliber(
        "road-biz", Mode.ROAD, "公路营业性客运量", "road-src",
        prior_year_total=10000, prior_year_days=3,
        deadline="2026-09-28T12:00:00",
    ))
    svc.define_caliber(Caliber(
        "road-car", Mode.ROAD, "非营业性小客车出行量", "road-src",
        prior_year_total=45000, prior_year_days=3,
        deadline="2026-09-28T12:00:00",
    ))
    svc.define_caliber(Caliber(
        "road-total", Mode.ROAD, "公路人员流动量", "road-src",
        kind="aggregate", children=("road-biz", "road-car"),
        prior_year_total=55000, prior_year_days=3,
        deadline="2026-09-28T12:00:00",
    ))
    svc.define_caliber(Caliber(
        "water", Mode.WATER, "水路客运量", "water-src",
        prior_year_total=300, prior_year_days=3,
        deadline="2026-09-28T12:00:00",
    ))
    svc.define_caliber(Caliber(
        "air", Mode.AIR, "民航客运量", "air-src",
        prior_year_total=500, prior_year_days=3,
        deadline="2026-09-28T12:00:00",
    ))
    return svc, str(path)


def standard_road_totals() -> dict[str, float]:
    return {DAYS[0]: 18500.0, DAYS[1]: 18200.0, DAYS[2]: 18800.0}
