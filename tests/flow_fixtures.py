"""客流归集服务测试的公共构造器。"""

from datetime import date, datetime, timezone
from decimal import Decimal

from src.flow_models import Batch, FlowRecord
from src.flow_service import FlowAggregationService
from src.flow_store import MemoryStore

PERIOD = "2026-mid-autumn"
REF_PERIOD = "2025-mid-autumn"
START = date(2026, 9, 25)
END = date(2026, 9, 27)
DAYS = (date(2026, 9, 25), date(2026, 9, 26), date(2026, 9, 27))
REF_START = date(2025, 9, 25)
REF_END = date(2025, 9, 27)
REF_DAYS = (date(2025, 9, 25), date(2025, 9, 26), date(2025, 9, 27))
AT = datetime(2026, 9, 28, 9, 0, tzinfo=timezone.utc)

SOURCES = {
    "rail": "铁路数据提供方",
    "road": "公路数据提供方",
    "water": "水路数据提供方",
    "air": "民航数据提供方",
}


def rec(mode, day, value, unit="万人次"):
    return FlowRecord(mode, day, Decimal(str(value)), unit)


def make_batch(
    batch_id,
    source,
    records,
    period=PERIOD,
    start=START,
    end=END,
    revises=None,
    reason=None,
    caliber_version=1,
):
    return Batch(
        batch_id=batch_id,
        source=source,
        caliber_id="interregional-flow",
        caliber_version=caliber_version,
        period_id=period,
        coverage_start=start,
        coverage_end=end,
        records=tuple(records),
        revision_reason=reason,
        revises=revises,
    )


def simple_batch(batch_id, source, mode, daily, days=DAYS, **kwargs):
    records = [rec(mode, day, value) for day, value in zip(days, daily)]
    return make_batch(batch_id, source, records, **kwargs)


def road_batch(batch_id="G1", factor=1, days=DAYS, **kwargs):
    records = []
    for day, commercial, private in zip(
        days, (100, 110, 120), (900, 950, 980)
    ):
        commercial *= factor
        private *= factor
        records += [
            rec("road", day, commercial + private),
            rec("road_commercial", day, commercial),
            rec("road_private", day, private),
        ]
    return make_batch(batch_id, SOURCES["road"], records, **kwargs)


def fill_all_modes(service, prefix="", factor=1, days=DAYS, **kwargs):
    """报送四种运输方式的完整批次，返回各方式批次编号。"""
    service.submit_batch(
        simple_batch(
            f"{prefix}R1", SOURCES["rail"], "rail",
            [v * factor for v in (5000, 5100, 5200)], days=days, **kwargs,
        ),
        AT,
    )
    service.submit_batch(
        road_batch(f"{prefix}G1", factor=factor, days=days, **kwargs), AT
    )
    service.submit_batch(
        simple_batch(
            f"{prefix}S1", SOURCES["water"], "water",
            [v * factor for v in (300, 310, 320)], days=days, **kwargs,
        ),
        AT,
    )
    service.submit_batch(
        simple_batch(
            f"{prefix}A1", SOURCES["air"], "air",
            [v * factor for v in (700, 720, 740)], days=days, **kwargs,
        ),
        AT,
    )


def make_service(store=None, compare_to=None):
    service = FlowAggregationService(store or MemoryStore())
    service.register_period(PERIOD, START, END, compare_to=compare_to)
    return service
