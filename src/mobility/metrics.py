"""派生指标纯函数：单位归一化、包含关系校验、日均/总量/可比增幅。

计算结果带 inputs 指纹；服务层据此判断哪些派生指标真正受补报影响，
未受影响的指标不重算、不产生新事件。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .errors import ContainmentError, ValidationError
from .models import (
    UNIT_FACTORS,
    BatchRecord,
    Caliber,
)

CONTAINMENT_TOLERANCE = 0.05  # 公路合计与分项之和允许 5 万人次（归一化后）绝对偏差


def normalize_values(raw: dict[str, float], unit: str) -> dict[str, float]:
    """把报送单位下的逐日数值折算为万人次。"""
    if unit not in UNIT_FACTORS:
        raise ValidationError(f"不支持的报送单位：{unit}（允许：{', '.join(UNIT_FACTORS)}）")
    if not raw:
        raise ValidationError("批次未包含任何逐日数据")
    factor = UNIT_FACTORS[unit]
    out: dict[str, float] = {}
    for day, value in raw.items():
        try:
            num = float(value)
        except (TypeError, ValueError):
            raise ValidationError(f"{day} 的数值无法解析：{value!r}")
        if num < 0:
            raise ValidationError(f"{day} 客流不能为负值")
        out[day] = round(num * factor, 6)
    return out


def check_coverage(values: dict[str, float], holiday: dict[str, Any]) -> None:
    """覆盖日期必须落在假期窗口内。"""
    start, end = holiday["start"], holiday["end"]
    for day in values:
        if not (start <= day <= end):
            raise ValidationError(f"报送日期 {day} 超出假期覆盖窗口 {start}~{end}")


def caliber_series(
    caliber: Caliber,
    accepted: list[BatchRecord],
    adjustments: list[dict[str, float]] | None = None,
) -> dict[str, float]:
    """汇总一个口径下所有已接受批次的逐日值（多批次按日并集，同日不得冲突）。

    adjustments: [{"day": "2026-09-25", "amount": 万人次增量, ...}] 已复核调整。
    """
    series: dict[str, float] = {}
    for b in accepted:
        for day, value in b.values.items():
            if day in series and abs(series[day] - value) > 1e-9:
                raise ValidationError(
                    f"口径 {caliber.caliber_id} 在 {day} 存在两个已接受的不同值；"
                    "修订请使用修订批次而非再次报送"
                )
            series[day] = value
    for adj in adjustments or []:
        day = adj["day"]
        series[day] = round(series.get(day, 0.0) + adj["amount"], 6)
    return series


def check_containment(
    aggregate: Caliber,
    children_series: list[tuple[str, dict[str, float]]],
    reported: dict[str, float] | None,
) -> dict[str, float]:
    """校验公路合计口径与营业性/非营业性分项的包含关系。

    - 来源直接报了合计：逐日 |合计 - (营业性 + 小客车)| <= 容差；
    - 未直接报合计：以分项之和作为合计。
    返回合计逐日序列。
    """
    days = sorted({d for _, s in children_series for d in s})
    summed = {d: round(sum(s.get(d, 0.0) for _, s in children_series), 6) for d in days}
    if reported is None:
        return summed
    for day in sorted(set(summed) | set(reported)):
        diff = abs(reported.get(day, 0.0) - summed.get(day, 0.0))
        if diff > CONTAINMENT_TOLERANCE:
            names = "、".join(name for name, _ in children_series)
            raise ContainmentError(
                f"{aggregate.name}在 {day} 的合计 {reported.get(day, 0.0)} 与"
                f"分项之和（{names}）{summed.get(day, 0.0)} 偏差 {diff:.2f} 万人次，"
                f"超过容差 {CONTAINMENT_TOLERANCE}"
            )
    # 校验通过时以来源合计为准（含来源对小缺口的口径修正），缺失日由分项补足
    result = dict(summed)
    result.update(reported)
    return result


def growth_rate(current_total: float, current_days: int, prior_total: float, prior_days: int) -> float:
    """可比增幅：假期天数不同时按日均比较。"""
    if prior_total <= 0 or prior_days <= 0 or current_days <= 0:
        raise ValidationError("可比基数无效")
    return round((current_total / current_days) / (prior_total / prior_days) - 1.0, 6)


def fingerprint(**parts: Any) -> str:
    payload = json.dumps(parts, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
