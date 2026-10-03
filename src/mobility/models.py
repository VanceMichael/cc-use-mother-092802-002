"""假期跨方式客流归集领域模型。

运输方式、统计口径、来源机构、原始批次、人工调整、发布版本与勘误。
所有客流金额在领域内部统一折算为 canonical unit（万人次）。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Literal

CANONICAL_UNIT = "万人次"

# 报送方可能使用的单位 -> 万人次换算系数
UNIT_FACTORS: dict[str, float] = {
    "万人次": 1.0,
    "人次": 1e-4,
    "亿人次": 1e4,
}


class Mode(str, Enum):
    RAIL = "rail"
    ROAD = "road"
    WATER = "water"
    AIR = "air"


MODE_LABELS = {
    Mode.RAIL: "铁路",
    Mode.ROAD: "公路",
    Mode.WATER: "水路",
    Mode.AIR: "民航",
}

CaliberKind = Literal["component", "aggregate"]
BatchStatus = Literal["accepted", "superseded", "rejected", "quarantined", "duplicate"]
AdjustmentStatus = Literal["pending", "approved", "rejected"]
TaskStatus = Literal["open", "completed", "cancelled"]


def content_hash(caliber_id: str, canonical_values: dict[str, float]) -> str:
    """批次语义内容摘要：口径 + 归一化后的逐日值。

    与报送单位无关——10000 人次与 1 万人次归一化后相同即视为同一内容。
    """
    payload = json.dumps(
        {"caliber": caliber_id, "values": canonical_values},
        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass
class Source:
    """数据来源机构，例如国家铁路局、公路科学研究院。"""

    source_id: str
    name: str


@dataclass
class Caliber:
    """统计口径。

    component 为直接采集的分项（如公路营业性客运量）；
    aggregate 为分项合计口径（如公路人员流动量 = 营业性 + 非营业性小客车），
    aggregate 口径本身也允许接收来源直接报送的合计值用于交叉校验。
    """

    caliber_id: str
    mode: Mode
    name: str
    source_id: str
    kind: CaliberKind = "component"
    children: tuple[str, ...] = ()
    unit: str = CANONICAL_UNIT
    # 上一假期可比基数（万人次）与假期天数，用于可比增幅
    prior_year_total: float | None = None
    prior_year_days: int | None = None
    incomparable_reason: str | None = None
    deadline: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.mode, Mode):
            self.mode = Mode(self.mode)
        if isinstance(self.deadline, str):
            self.deadline = datetime.fromisoformat(self.deadline)

    def to_dict(self) -> dict:
        return {
            "caliber_id": self.caliber_id,
            "mode": self.mode.value,
            "name": self.name,
            "source_id": self.source_id,
            "kind": self.kind,
            "children": list(self.children),
            "unit": self.unit,
            "prior_year_total": self.prior_year_total,
            "prior_year_days": self.prior_year_days,
            "incomparable_reason": self.incomparable_reason,
            "deadline": self.deadline.isoformat() if self.deadline else None,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "Caliber":
        return cls(
            caliber_id=raw["caliber_id"],
            mode=Mode(raw["mode"]),
            name=raw["name"],
            source_id=raw["source_id"],
            kind=raw.get("kind", "component"),
            children=tuple(raw.get("children", ())),
            unit=raw.get("unit", CANONICAL_UNIT),
            prior_year_total=raw.get("prior_year_total"),
            prior_year_days=raw.get("prior_year_days"),
            incomparable_reason=raw.get("incomparable_reason"),
            deadline=datetime.fromisoformat(raw["deadline"]) if raw.get("deadline") else None,
        )


@dataclass
class BatchRecord:
    """一次来源报送的原始批次（不可篡改，只可被修订批次接替）。"""

    batch_id: str
    source_id: str
    caliber_id: str
    values: dict[str, float]  # 归一化后逐日值（万人次）
    reported_unit: str
    content_hash: str
    received_at: datetime
    status: BatchStatus
    revision_of: str | None = None
    revision_reason: str | None = None
    duplicate_of: str | None = None
    conflicting_with: str | None = None
    superseded_by: str | None = None
    resolution: str | None = None
    display_id: str | None = None  # 隔离批次保留的原编号

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "source_id": self.source_id,
            "caliber_id": self.caliber_id,
            "values": self.values,
            "reported_unit": self.reported_unit,
            "content_hash": self.content_hash,
            "received_at": self.received_at.isoformat(),
            "status": self.status,
            "revision_of": self.revision_of,
            "revision_reason": self.revision_reason,
            "duplicate_of": self.duplicate_of,
            "conflicting_with": self.conflicting_with,
            "superseded_by": self.superseded_by,
            "resolution": self.resolution,
            "display_id": self.display_id,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "BatchRecord":
        return cls(
            batch_id=raw["batch_id"],
            source_id=raw["source_id"],
            caliber_id=raw["caliber_id"],
            values={k: float(v) for k, v in raw["values"].items()},
            reported_unit=raw["reported_unit"],
            content_hash=raw["content_hash"],
            received_at=datetime.fromisoformat(raw["received_at"]),
            status=raw["status"],
            revision_of=raw.get("revision_of"),
            revision_reason=raw.get("revision_reason"),
            duplicate_of=raw.get("duplicate_of"),
            conflicting_with=raw.get("conflicting_with"),
            superseded_by=raw.get("superseded_by"),
            resolution=raw.get("resolution"),
            display_id=raw.get("display_id"),
        )


@dataclass
class Adjustment:
    """人工调整：delta 为万人次增量（可为负），生效前必须由另一人复核。"""

    adjustment_id: str
    target: Literal["caliber", "national"]
    amount: float
    reason: str
    created_by: str
    created_at: datetime
    status: AdjustmentStatus = "pending"
    caliber_id: str | None = None
    day: str | None = None
    reviewed_by: str | None = None
    reviewed_at: datetime | None = None
    review_note: str | None = None

    def to_dict(self) -> dict:
        return {
            "adjustment_id": self.adjustment_id,
            "target": self.target,
            "amount": self.amount,
            "reason": self.reason,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "status": self.status,
            "caliber_id": self.caliber_id,
            "day": self.day,
            "reviewed_by": self.reviewed_by,
            "reviewed_at": self.reviewed_at.isoformat() if self.reviewed_at else None,
            "review_note": self.review_note,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "Adjustment":
        return cls(
            adjustment_id=raw["adjustment_id"],
            target=raw["target"],
            amount=float(raw["amount"]),
            reason=raw["reason"],
            created_by=raw["created_by"],
            created_at=datetime.fromisoformat(raw["created_at"]),
            status=raw.get("status", "pending"),
            caliber_id=raw.get("caliber_id"),
            day=raw.get("day"),
            reviewed_by=raw.get("reviewed_by"),
            reviewed_at=datetime.fromisoformat(raw["reviewed_at"]) if raw.get("reviewed_at") else None,
            review_note=raw.get("review_note"),
        )


@dataclass
class PublishTask:
    """待发布任务：持久化后服务恢复仍可继续。"""

    task_id: str
    title: str
    due: datetime
    created_by: str
    created_at: datetime
    status: TaskStatus = "open"
    completed_at: datetime | None = None

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "due": self.due.isoformat(),
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "status": self.status,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "PublishTask":
        return cls(
            task_id=raw["task_id"],
            title=raw["title"],
            due=datetime.fromisoformat(raw["due"]),
            created_by=raw["created_by"],
            created_at=datetime.fromisoformat(raw["created_at"]),
            status=raw.get("status", "open"),
            completed_at=datetime.fromisoformat(raw["completed_at"]) if raw.get("completed_at") else None,
        )
