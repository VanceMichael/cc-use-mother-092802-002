"""统计口径注册与分项包含关系校验。

口径以树形结构描述分项之间的包含关系，例如：

    total ─┬─ rail
           ├─ road ─┬─ road_commercial（营业性客运）
           │        └─ road_private（非营业性小客车）
           ├─ water
           └─ air

校验规则：
- 报送分项必须属于口径树；
- 父项与子项同时报送时，子项合计不得超出容差，且子项必须报全；
- 单位必须能换算为万人次（人数类单位直接拒绝）。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from .flow_models import Batch, UnitError

DEFAULT_CALIBER_ID = "interregional-flow"
DEFAULT_TREE: dict[str, tuple[str, ...]] = {
    "total": ("rail", "road", "water", "air"),
    "road": ("road_commercial", "road_private"),
}
DEFAULT_TOLERANCE = Decimal("0.5")  # 万人次，容纳四舍五入尾差


@dataclass(frozen=True)
class Caliber:
    """一个版本的统计口径：分项包含树与汇总容差。"""

    caliber_id: str
    version: int
    tree: dict[str, tuple[str, ...]]
    tolerance: Decimal = DEFAULT_TOLERANCE

    def children_of(self, mode: str) -> tuple[str, ...]:
        return self.tree.get(mode, ())

    def known(self, mode: str) -> bool:
        if mode in self.tree:
            return True
        return any(mode in children for children in self.tree.values())

    def leaf_modes(self) -> tuple[str, ...]:
        """total 的直接子项，即跨方式汇总的基本分项。"""
        return self.children_of("total")

    def leaf_under_total(self, mode: str) -> str | None:
        """把任意分项归并到 total 的直接子项；不在树中返回 None。"""
        if not self.known(mode):
            return None
        current = mode
        while current != "total":
            for parent, children in self.tree.items():
                if current in children:
                    if parent == "total":
                        return current
                    current = parent
                    break
            else:  # 不可达，known 已保证不会发生
                return None
        return "total"

    def to_dict(self) -> dict[str, Any]:
        return {
            "caliber_id": self.caliber_id,
            "version": self.version,
            "tree": {k: list(v) for k, v in self.tree.items()},
            "tolerance": str(self.tolerance),
        }

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "Caliber":
        return Caliber(
            caliber_id=raw["caliber_id"],
            version=int(raw["version"]),
            tree={k: tuple(v) for k, v in raw["tree"].items()},
            tolerance=Decimal(raw.get("tolerance", str(DEFAULT_TOLERANCE))),
        )


class CaliberRegistry:
    """按（口径标识, 版本）登记口径，版本只增不减。"""

    def __init__(self) -> None:
        self._calibers: dict[tuple[str, int], Caliber] = {}

    def register(self, caliber: Caliber) -> None:
        key = (caliber.caliber_id, caliber.version)
        existing = self._calibers.get(key)
        if existing is not None and existing != caliber:
            raise ValueError(f"口径{caliber.caliber_id}版本{caliber.version}已存在且内容不同")
        self._calibers[key] = caliber

    def get(self, caliber_id: str, version: int) -> Caliber | None:
        return self._calibers.get((caliber_id, version))

    def latest(self, caliber_id: str) -> Caliber | None:
        versions = [c for (cid, _), c in self._calibers.items() if cid == caliber_id]
        return max(versions, key=lambda c: c.version, default=None)

    def all(self) -> list[Caliber]:
        return sorted(self._calibers.values(), key=lambda c: (c.caliber_id, c.version))


def validate_batch(
    batch: Batch,
    caliber: Caliber,
    period_start: date,
    period_end: date,
) -> list[str]:
    """校验批次的覆盖日期、单位与分项包含关系，返回问题列表（空为通过）。"""
    issues: list[str] = []
    if batch.coverage_end < batch.coverage_start:
        issues.append("覆盖日期起止颠倒")
    if batch.coverage_start < period_start or batch.coverage_end > period_end:
        issues.append("覆盖日期超出假期范围")

    by_day: dict[date, dict[str, Decimal]] = {}
    for record in batch.records:
        if not caliber.known(record.mode):
            issues.append(f"未知分项{record.mode}")
            continue
        try:
            value = record.canonical()
        except UnitError as exc:
            issues.append(str(exc))
            continue
        if not (batch.coverage_start <= record.day <= batch.coverage_end):
            issues.append(f"{record.day.isoformat()}不在批次覆盖范围内")
            continue
        day_values = by_day.setdefault(record.day, {})
        if record.mode in day_values:
            issues.append(f"{record.day.isoformat()}分项{record.mode}重复报送")
            continue
        day_values[record.mode] = value

    for day, values in sorted(by_day.items()):
        label = day.isoformat()
        for parent, children in caliber.tree.items():
            if parent == "total":
                continue  # 跨方式总量由服务汇总，单一来源不校验
            present = [c for c in children if c in values]
            if parent in values and present:
                if len(present) < len(children):
                    missing = "、".join(c for c in children if c not in values)
                    issues.append(f"{label}分项{parent}缺少子项{missing}")
                    continue
                subtotal = sum(values[c] for c in children)
                if abs(values[parent] - subtotal) > caliber.tolerance:
                    issues.append(
                        f"{label}分项{parent}为{values[parent]}万人次，"
                        f"与子项合计{subtotal}万人次超出容差"
                    )
            if parent in values:
                for child in present:
                    if values[child] - values[parent] > caliber.tolerance:
                        issues.append(f"{label}子项{child}超过父项{parent}")
    return issues
