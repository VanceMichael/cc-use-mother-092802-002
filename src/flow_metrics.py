"""派生指标计算：分方式汇总、日均、假期总量与可比增幅。

计算均为纯函数，输入是已纳入汇总的批次与已复核的人工调整；
增量重算由服务层按受影响分项调用，未受影响的分项沿用缓存结果。
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from .flow_caliber import CaliberRegistry
from .flow_models import (
    APPROVED,
    Adjustment,
    Batch,
    Growth,
    ModeAggregate,
    PeriodMetrics,
)

ZERO = Decimal("0")


def mode_daily_values(
    mode: str,
    batches: list[Batch],
    registry: CaliberRegistry,
) -> dict[date, Decimal]:
    """某方式分项的逐日万人次：父项直报优先，否则子项求和。"""
    daily: dict[date, Decimal] = {}
    for batch in batches:
        caliber = registry.get(batch.caliber_id, batch.caliber_version)
        if caliber is None:
            continue
        children = caliber.children_of(mode)
        by_mode: dict[str, dict[date, Decimal]] = {}
        for record in batch.records:
            leaf = caliber.leaf_under_total(record.mode)
            if leaf != mode and record.mode != mode:
                continue
            by_mode.setdefault(record.mode, {})[record.day] = record.canonical()
        days: set[date] = set()
        for values in by_mode.values():
            days.update(values)
        for day in days:
            if day in by_mode.get(mode, {}):
                value = by_mode[mode][day]
            else:
                parts = [by_mode[c][day] for c in children if day in by_mode.get(c, {})]
                if not parts:
                    continue
                value = sum(parts, ZERO)
            daily[day] = daily.get(day, ZERO) + value
    return daily


def _batch_ref(batch: Batch) -> dict[str, object]:
    return {
        "batch_id": batch.batch_id,
        "source": batch.source,
        "caliber_id": batch.caliber_id,
        "caliber_version": batch.caliber_version,
        "revision_reason": batch.revision_reason,
        "fingerprint": batch.fingerprint(),
    }


def compute_mode_aggregate(
    period_id: str,
    days: int,
    mode: str,
    batches: list[Batch],
    adjustments: list[Adjustment],
    registry: CaliberRegistry,
) -> ModeAggregate:
    """汇总单个分项：批次求和后叠加已复核的人工调整。"""
    daily = mode_daily_values(mode, batches, registry)
    total = sum(daily.values(), ZERO)
    applied = [a for a in adjustments if a.period_id == period_id and a.mode == mode and a.status == APPROVED]
    for adjustment in applied:
        total += adjustment.delta
    contributing = [b for b in batches if _touches(b, mode, registry)]
    caliber_id = contributing[-1].caliber_id if contributing else ""
    caliber_version = max((b.caliber_version for b in contributing), default=0)
    return ModeAggregate(
        period_id=period_id,
        mode=mode,
        total=total,
        daily_average=(total / days) if days else ZERO,
        days=days,
        batches=tuple(_batch_ref(b) for b in contributing),
        adjustment_ids=tuple(a.adjustment_id for a in applied),
        caliber_id=caliber_id,
        caliber_version=caliber_version,
    )


def _touches(batch: Batch, mode: str, registry: CaliberRegistry) -> bool:
    caliber = registry.get(batch.caliber_id, batch.caliber_version)
    if caliber is None:
        return False
    return any(caliber.leaf_under_total(record.mode) == mode for record in batch.records)


def compute_growth(
    current_total: Decimal,
    reference_total: Decimal,
    current_calibers: set[tuple[str, int]],
    reference_calibers: set[tuple[str, int]],
    reference_period_id: str,
) -> Growth:
    """可比增幅：两期口径版本一致才可比，否则只给出说明。"""
    if current_calibers != reference_calibers:
        return Growth(
            reference_period_id=reference_period_id,
            comparable=False,
            current_total=current_total,
            reference_total=reference_total,
            note="两期统计口径版本不一致，增幅不可比",
        )
    if reference_total == ZERO:
        return Growth(
            reference_period_id=reference_period_id,
            comparable=False,
            current_total=current_total,
            reference_total=reference_total,
            note="参照期总量为零，无法计算增幅",
        )
    rate = ((current_total - reference_total) / reference_total * 100).quantize(Decimal("0.0001"))
    return Growth(
        reference_period_id=reference_period_id,
        comparable=True,
        current_total=current_total,
        reference_total=reference_total,
        rate=rate,
        note="口径一致，增幅可比",
    )


def compute_period_metrics(
    period_id: str,
    days: int,
    aggregates: dict[str, ModeAggregate],
    reference_period_id: str | None = None,
    reference_aggregates: dict[str, ModeAggregate] | None = None,
) -> PeriodMetrics:
    """由各分项汇总出假期总量、日均，并与参照期计算可比增幅。"""
    holiday_total = sum((a.total for a in aggregates.values()), ZERO)
    growth = None
    if reference_period_id and reference_aggregates:
        reference_total = sum((a.total for a in reference_aggregates.values()), ZERO)
        current_calibers = {(a.caliber_id, a.caliber_version) for a in aggregates.values()}
        reference_calibers = {(a.caliber_id, a.caliber_version) for a in reference_aggregates.values()}
        growth = compute_growth(
            holiday_total, reference_total, current_calibers, reference_calibers, reference_period_id
        )
    return PeriodMetrics(
        period_id=period_id,
        days=days,
        modes=dict(aggregates),
        holiday_total=holiday_total,
        holiday_daily_average=(holiday_total / days) if days else ZERO,
        growth=growth,
    )
