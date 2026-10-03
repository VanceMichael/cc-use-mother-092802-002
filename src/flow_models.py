"""客流归集服务的核心数据结构：单位换算、批次、调整、版本与任务。

所有数量在计算前统一换算为万人次（CANONICAL_UNIT），原始报送单位保留在
记录中用于溯源。各结构均提供 to_dict/from_dict，便于持久化与服务恢复。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any

CANONICAL_UNIT = "万人次"

# 可换算到万人次的报送单位；`万人` 是人数而非人次，语义不同，禁止混入。
UNIT_FACTORS: dict[str, Decimal] = {
    "人次": Decimal("0.0001"),
    "万人次": Decimal("1"),
}
INCOMPATIBLE_UNITS = frozenset({"万人"})

# 批次状态
INCLUDED = "included"      # 参与汇总
SUPERSEDED = "superseded"  # 被修订批次取代

# 人工调整状态
PENDING = "pending"
APPROVED = "approved"
ADJ_REJECTED = "rejected"

# 任务类型与状态
REMINDER = "deadline-reminder"
PUBLISH_TASK = "pending-publish"
OPEN = "open"
DONE = "done"


class UnitError(ValueError):
    """报送单位无法换算为万人次。"""


def to_canonical(value: Decimal, unit: str) -> Decimal:
    """把报送值换算为万人次；人数类单位一律拒绝。"""
    if unit in INCOMPATIBLE_UNITS:
        raise UnitError(f"单位{unit}是人数而非人次，不能混入口径")
    factor = UNIT_FACTORS.get(unit)
    if factor is None:
        raise UnitError(f"未知单位{unit}")
    return value * factor


@dataclass(frozen=True)
class FlowRecord:
    """单日单分项的原始报送值，value 与 unit 均为报送原样。"""

    mode: str
    day: date
    value: Decimal
    unit: str = CANONICAL_UNIT

    def canonical(self) -> Decimal:
        """换算为万人次，单位不合法时抛出 UnitError。"""
        return to_canonical(self.value, self.unit)

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "day": self.day.isoformat(),
            "value": str(self.value),
            "unit": self.unit,
        }

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "FlowRecord":
        return FlowRecord(
            mode=raw["mode"],
            day=date.fromisoformat(raw["day"]),
            value=Decimal(raw["value"]),
            unit=raw.get("unit", CANONICAL_UNIT),
        )


@dataclass
class Batch:
    """一个来源机构报送的一批原始数据。

    revises 指向被修订的批次编号，revision_reason 记录修订原因；
    fingerprint 只覆盖数据内容，与接收时间、状态无关，用于识别重复报送。
    """

    batch_id: str
    source: str
    caliber_id: str
    caliber_version: int
    period_id: str
    coverage_start: date
    coverage_end: date
    records: tuple[FlowRecord, ...]
    revision_reason: str | None = None
    revises: str | None = None
    status: str = INCLUDED
    received_at: str = ""

    def fingerprint(self) -> str:
        payload = {
            "batch_id": self.batch_id,
            "source": self.source,
            "caliber_id": self.caliber_id,
            "caliber_version": self.caliber_version,
            "period_id": self.period_id,
            "coverage_start": self.coverage_start.isoformat(),
            "coverage_end": self.coverage_end.isoformat(),
            "records": sorted(
                (r.to_dict() for r in self.records),
                key=lambda d: (d["mode"], d["day"]),
            ),
            "revises": self.revises,
        }
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def modes_touched(self) -> set[str]:
        return {r.mode for r in self.records}

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "source": self.source,
            "caliber_id": self.caliber_id,
            "caliber_version": self.caliber_version,
            "period_id": self.period_id,
            "coverage_start": self.coverage_start.isoformat(),
            "coverage_end": self.coverage_end.isoformat(),
            "records": [r.to_dict() for r in self.records],
            "revision_reason": self.revision_reason,
            "revises": self.revises,
            "status": self.status,
            "received_at": self.received_at,
        }

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "Batch":
        return Batch(
            batch_id=raw["batch_id"],
            source=raw["source"],
            caliber_id=raw["caliber_id"],
            caliber_version=int(raw["caliber_version"]),
            period_id=raw["period_id"],
            coverage_start=date.fromisoformat(raw["coverage_start"]),
            coverage_end=date.fromisoformat(raw["coverage_end"]),
            records=tuple(FlowRecord.from_dict(r) for r in raw["records"]),
            revision_reason=raw.get("revision_reason"),
            revises=raw.get("revises"),
            status=raw.get("status", INCLUDED),
            received_at=raw.get("received_at", ""),
        )


@dataclass
class QuarantineEntry:
    """被隔离的批次：编号冲突或校验未通过，解决前不参与汇总。"""

    quarantine_id: str
    batch: Batch
    reasons: tuple[str, ...]
    opened_at: str
    status: str = OPEN  # open / resolved
    resolution: str | None = None
    resolved_by: str | None = None
    resolved_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "quarantine_id": self.quarantine_id,
            "batch": self.batch.to_dict(),
            "reasons": list(self.reasons),
            "opened_at": self.opened_at,
            "status": self.status,
            "resolution": self.resolution,
            "resolved_by": self.resolved_by,
            "resolved_at": self.resolved_at,
        }

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "QuarantineEntry":
        return QuarantineEntry(
            quarantine_id=raw["quarantine_id"],
            batch=Batch.from_dict(raw["batch"]),
            reasons=tuple(raw["reasons"]),
            opened_at=raw["opened_at"],
            status=raw.get("status", OPEN),
            resolution=raw.get("resolution"),
            resolved_by=raw.get("resolved_by"),
            resolved_at=raw.get("resolved_at"),
        )


@dataclass
class Adjustment:
    """人工调整：必须经另一名人员复核（approved）后才参与汇总。"""

    adjustment_id: str
    period_id: str
    mode: str
    delta: Decimal
    reason: str
    proposed_by: str
    proposed_at: str
    day: date | None = None
    status: str = PENDING
    reviewed_by: str | None = None
    reviewed_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "adjustment_id": self.adjustment_id,
            "period_id": self.period_id,
            "mode": self.mode,
            "delta": str(self.delta),
            "reason": self.reason,
            "proposed_by": self.proposed_by,
            "proposed_at": self.proposed_at,
            "day": self.day.isoformat() if self.day else None,
            "status": self.status,
            "reviewed_by": self.reviewed_by,
            "reviewed_at": self.reviewed_at,
        }

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "Adjustment":
        return Adjustment(
            adjustment_id=raw["adjustment_id"],
            period_id=raw["period_id"],
            mode=raw["mode"],
            delta=Decimal(raw["delta"]),
            reason=raw["reason"],
            proposed_by=raw["proposed_by"],
            proposed_at=raw["proposed_at"],
            day=date.fromisoformat(raw["day"]) if raw.get("day") else None,
            status=raw.get("status", PENDING),
            reviewed_by=raw.get("reviewed_by"),
            reviewed_at=raw.get("reviewed_at"),
        )


@dataclass
class Task:
    """截止期提醒或待发布任务，持久化后服务恢复可继续跟踪。"""

    task_id: str
    kind: str  # REMINDER / PUBLISH_TASK
    period_id: str
    due: date
    message: str
    source: str | None = None
    mode: str | None = None
    status: str = OPEN

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "kind": self.kind,
            "period_id": self.period_id,
            "due": self.due.isoformat(),
            "message": self.message,
            "source": self.source,
            "mode": self.mode,
            "status": self.status,
        }

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "Task":
        return Task(
            task_id=raw["task_id"],
            kind=raw["kind"],
            period_id=raw["period_id"],
            due=date.fromisoformat(raw["due"]),
            message=raw["message"],
            source=raw.get("source"),
            mode=raw.get("mode"),
            status=raw.get("status", OPEN),
        )


@dataclass
class ModeAggregate:
    """单个运输方式分项在假期内的派生指标。"""

    period_id: str
    mode: str
    total: Decimal
    daily_average: Decimal
    days: int
    batches: tuple[dict[str, Any], ...] = ()  # 采用的批次溯源信息
    adjustment_ids: tuple[str, ...] = ()
    caliber_id: str = ""
    caliber_version: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "period_id": self.period_id,
            "mode": self.mode,
            "total": str(self.total),
            "daily_average": str(self.daily_average),
            "days": self.days,
            "batches": list(self.batches),
            "adjustment_ids": list(self.adjustment_ids),
            "caliber_id": self.caliber_id,
            "caliber_version": self.caliber_version,
        }

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "ModeAggregate":
        return ModeAggregate(
            period_id=raw["period_id"],
            mode=raw["mode"],
            total=Decimal(raw["total"]),
            daily_average=Decimal(raw["daily_average"]),
            days=int(raw["days"]),
            batches=tuple(raw.get("batches", ())),
            adjustment_ids=tuple(raw.get("adjustment_ids", ())),
            caliber_id=raw.get("caliber_id", ""),
            caliber_version=int(raw.get("caliber_version", 0)),
        )


@dataclass
class Growth:
    """可比增幅：口径一致时才给出 rate，否则说明不可比原因。"""

    reference_period_id: str
    comparable: bool
    current_total: Decimal
    reference_total: Decimal
    rate: Decimal | None = None
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "reference_period_id": self.reference_period_id,
            "comparable": self.comparable,
            "current_total": str(self.current_total),
            "reference_total": str(self.reference_total),
            "rate": str(self.rate) if self.rate is not None else None,
            "note": self.note,
        }

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "Growth":
        return Growth(
            reference_period_id=raw["reference_period_id"],
            comparable=bool(raw["comparable"]),
            current_total=Decimal(raw["current_total"]),
            reference_total=Decimal(raw["reference_total"]),
            rate=Decimal(raw["rate"]) if raw.get("rate") is not None else None,
            note=raw.get("note", ""),
        )


@dataclass
class PeriodMetrics:
    """一个假期的全部派生指标：分方式汇总、总量、日均与可比增幅。"""

    period_id: str
    days: int
    modes: dict[str, ModeAggregate]
    holiday_total: Decimal
    holiday_daily_average: Decimal
    growth: Growth | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "period_id": self.period_id,
            "days": self.days,
            "modes": {m: a.to_dict() for m, a in self.modes.items()},
            "holiday_total": str(self.holiday_total),
            "holiday_daily_average": str(self.holiday_daily_average),
            "growth": self.growth.to_dict() if self.growth else None,
        }

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "PeriodMetrics":
        return PeriodMetrics(
            period_id=raw["period_id"],
            days=int(raw["days"]),
            modes={m: ModeAggregate.from_dict(a) for m, a in raw["modes"].items()},
            holiday_total=Decimal(raw["holiday_total"]),
            holiday_daily_average=Decimal(raw["holiday_daily_average"]),
            growth=Growth.from_dict(raw["growth"]) if raw.get("growth") else None,
        )


@dataclass
class PublishedVersion:
    """对外发布的版本，发布后不可变；更正通过 errata_of 链接新版本。"""

    version_id: str
    period_id: str
    sequence: int
    published_at: str
    metrics: PeriodMetrics
    lineage: dict[str, Any]  # mode -> {batches, adjustment_ids, caliber}
    pending: tuple[str, ...] = ()
    note: str = ""
    errata_of: str | None = None
    errata_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "version_id": self.version_id,
            "period_id": self.period_id,
            "sequence": self.sequence,
            "published_at": self.published_at,
            "metrics": self.metrics.to_dict(),
            "lineage": self.lineage,
            "pending": list(self.pending),
            "note": self.note,
            "errata_of": self.errata_of,
            "errata_reason": self.errata_reason,
        }

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "PublishedVersion":
        return PublishedVersion(
            version_id=raw["version_id"],
            period_id=raw["period_id"],
            sequence=int(raw["sequence"]),
            published_at=raw["published_at"],
            metrics=PeriodMetrics.from_dict(raw["metrics"]),
            lineage=raw["lineage"],
            pending=tuple(raw.get("pending", ())),
            note=raw.get("note", ""),
            errata_of=raw.get("errata_of"),
            errata_reason=raw.get("errata_reason"),
        )


@dataclass
class IngestResult:
    """报送受理结果：included / duplicate / revised / quarantined。"""

    status: str
    batch: Batch
    issues: tuple[str, ...] = ()
    quarantine_id: str | None = None
    affected_modes: tuple[str, ...] = ()


@dataclass
class Explanation:
    """对任一历史发布版本的解释：采用了哪些批次、哪些数据待确认、
    与前一版公报的差异及原因。"""

    version_id: str
    period_id: str
    published_at: str
    batches_used: tuple[dict[str, Any], ...]
    pending: tuple[str, ...]
    adjustments: tuple[dict[str, Any], ...]
    differs_from: str | None
    differences: tuple[str, ...] = ()
    errata_of: str | None = None
    errata_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "version_id": self.version_id,
            "period_id": self.period_id,
            "published_at": self.published_at,
            "batches_used": list(self.batches_used),
            "pending": list(self.pending),
            "adjustments": list(self.adjustments),
            "differs_from": self.differs_from,
            "differences": list(self.differences),
            "errata_of": self.errata_of,
            "errata_reason": self.errata_reason,
        }
