"""客流归集服务：批次归集、校验隔离、增量重算、发布勘误、双人复核、恢复续跑。"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from .errors import (
    ContainmentError,
    NotFoundError,
    QuarantineError,
    ValidationError,
    WorkflowError,
)
from .eventstore import EventStore, now
from .metrics import (
    caliber_series,
    check_containment,
    check_coverage,
    fingerprint,
    growth_rate,
    normalize_values,
)
from .models import (
    CANONICAL_UNIT,
    MODE_LABELS,
    Adjustment,
    BatchRecord,
    Caliber,
    Mode,
    PublishTask,
    Source,
    content_hash,
)
from .state import apply_event, from_jsonable, initial_state, to_jsonable


class AggregationService:
    """基于事件日志的归集服务；构造时回放即完成崩溃恢复。"""

    def __init__(self, store_dir: str):
        self.store = EventStore(store_dir)
        self.state = initial_state()
        snap = self.store.load_snapshot()
        if snap:
            self.state = from_jsonable(snap["state"])
            after = snap["last_seq"]
        else:
            after = 0
        for event in self.store.iter_events(after):
            apply_event(self.state, event)

    # ================= 基础档案 =================
    def open_holiday(self, name: str, start: str, end: str) -> None:
        if self.state["holiday"]:
            raise WorkflowError("假期窗口已开启，不可重复开启")
        d0, d1 = datetime.fromisoformat(start).date(), datetime.fromisoformat(end).date()
        if d1 < d0:
            raise ValidationError("假期结束日期早于开始日期")
        days = (d1 - d0).days + 1
        self._emit("holiday_opened", {"name": name, "start": start, "end": end, "days": days})

    def register_source(self, source_id: str, name: str) -> Source:
        if source_id in self.state["sources"]:
            raise WorkflowError(f"来源机构已存在：{source_id}")
        s = Source(source_id=source_id, name=name)
        self._emit("source_registered", {"source_id": s.source_id, "name": s.name})
        return s

    def define_caliber(self, caliber: Caliber) -> Caliber:
        self._require_holiday()
        if caliber.caliber_id in self.state["calibers"]:
            raise WorkflowError(f"统计口径已存在：{caliber.caliber_id}")
        if caliber.source_id not in self.state["sources"]:
            raise ValidationError("口径引用了未登记的来源机构")
        for child in caliber.children:
            if child not in self.state["calibers"]:
                raise ValidationError(f"合计口径引用了不存在的分项：{child}")
        self._emit("caliber_defined", caliber.to_dict())
        return caliber

    # ================= 批次归集 =================
    def submit_batch(
        self,
        batch_id: str,
        source_id: str,
        caliber_id: str,
        values: dict[str, float],
        unit: str = CANONICAL_UNIT,
        revision_of: str | None = None,
        revision_reason: str | None = None,
    ) -> BatchRecord:
        """接收一次来源报送。

        - 同来源同编号同内容：判重，不重复累计；
        - 同来源同编号不同内容且非显式修订：隔离，等待人工裁决；
        - 显式修订（revision_of=同编号）：旧批次被接替，修订原因留痕；
        - 先校验单位、覆盖日期与口径归属；合计/分项齐备时校验包含关系。
        """
        holiday = self._require_holiday()
        if source_id not in self.state["sources"]:
            raise ValidationError(f"未登记的来源机构：{source_id}")
        caliber = self.state["calibers"].get(caliber_id)
        if caliber is None:
            raise NotFoundError(f"统计口径不存在：{caliber_id}")
        if caliber.source_id != source_id:
            raise ValidationError(f"来源 {source_id} 无权报送口径 {caliber_id}（归属 {caliber.source_id}）")
        canonical = normalize_values(values, unit)
        check_coverage(canonical, holiday)
        # 不同批次编号对同一日期不得给出冲突值；同编号内容不同走隔离/修订路径
        if revision_of is None:
            for b in self._accepted_batches(caliber):
                if b.display_id == batch_id:
                    continue
                clash = sorted(set(b.values) & set(canonical))
                if clash and any(abs(b.values[d] - canonical[d]) > 1e-9 for d in clash):
                    raise ValidationError(
                        f"口径 {caliber_id} 在 {clash[0]} 已由批次 {b.display_id} 报送不同值；"
                        "请使用同编号修订批次并注明原因，不得另起编号覆盖")
        chash = content_hash(caliber_id, canonical)
        idx_key = f"{source_id}/{batch_id}"

        existing_id = self.state["display_index"].get(idx_key)
        if existing_id is not None:
            existing = self.state["batches"][existing_id]
            if existing.content_hash == chash:
                dup = self._new_duplicate(batch_id, source_id, caliber_id, canonical, unit, chash, existing)
                self._emit("batch_duplicated", {"batch": dup.to_dict(), "duplicate_of": existing.batch_id})
                return dup
            if revision_of != batch_id:
                self._quarantine(batch_id, source_id, caliber_id, canonical, unit, chash, existing)
            if not revision_reason:
                raise ValidationError("修订批次必须填写修订原因")
            if set(existing.values) - set(canonical):
                raise ValidationError("修订批次必须覆盖原批次的全部报送日期")
            # 写入前试算包含关系；不成立则整笔拒绝，状态不被污染
            try:
                self._containment_precheck(caliber, canonical, exclude_batch_id=existing.batch_id)
            except ContainmentError:
                self._reject_before_accept(
                    batch_id, source_id, caliber_id, canonical, unit, chash,
                    "包含关系校验失败，退回来源核实")
                raise
            # 显式修订：接替旧批次
            new_id = self._unique_internal_id(f"{source_id}/{batch_id}", "r")
            record = BatchRecord(
                batch_id=new_id, display_id=batch_id, source_id=source_id, caliber_id=caliber_id,
                values=canonical, reported_unit=unit, content_hash=chash,
                received_at=datetime.fromisoformat(now()), status="accepted",
                revision_of=existing.batch_id, revision_reason=revision_reason,
            )
            self._emit("batch_superseded", {"batch": record.to_dict(), "superseded_id": existing.batch_id})
            self._recompute_affected([caliber.caliber_id])
            return record

        if revision_of is not None:
            raise NotFoundError(f"修订目标批次不存在：{revision_of}")

        try:
            self._containment_precheck(caliber, canonical)
        except ContainmentError:
            self._reject_before_accept(
                batch_id, source_id, caliber_id, canonical, unit, chash,
                "包含关系校验失败，退回来源核实")
            raise
        record = BatchRecord(
            batch_id=self._unique_internal_id(f"{source_id}/{batch_id}", "b"),
            display_id=batch_id,
            source_id=source_id, caliber_id=caliber_id, values=canonical,
            reported_unit=unit, content_hash=chash,
            received_at=datetime.fromisoformat(now()), status="accepted",
        )
        self._emit("batch_received", {"batch": record.to_dict()})
        self._recompute_affected([caliber.caliber_id])
        return record

    def _new_duplicate(self, display_id: str, source_id: str, caliber_id: str,
                       values: dict[str, float], unit: str, chash: str,
                       existing: BatchRecord) -> BatchRecord:
        n = sum(1 for b in self.state["batches"].values() if b.duplicate_of == existing.batch_id) + 1
        return BatchRecord(
            batch_id=f"{source_id}/{display_id}#dup{n}", display_id=display_id, source_id=source_id,
            caliber_id=caliber_id, values=values, reported_unit=unit, content_hash=chash,
            received_at=datetime.fromisoformat(now()), status="duplicate",
            duplicate_of=existing.batch_id,
        )

    def _quarantine(self, display_id: str, source_id: str, caliber_id: str,
                    values: dict[str, float], unit: str, chash: str,
                    existing: BatchRecord) -> None:
        qid = self._unique_internal_id(f"{source_id}/{display_id}", "q")
        q = BatchRecord(
            batch_id=qid, display_id=display_id, source_id=source_id, caliber_id=caliber_id,
            values=values, reported_unit=unit, content_hash=chash,
            received_at=datetime.fromisoformat(now()), status="quarantined",
            conflicting_with=existing.batch_id,
        )
        self._emit("batch_quarantined", {"batch": q.to_dict()})
        raise QuarantineError(
            f"批次 {display_id} 与已接收内容不同，已隔离为 {qid}，等待裁决（不得静默覆盖）",
            quarantined_id=qid, display_id=display_id, conflicting_with=existing.batch_id,
        )

    def resolve_quarantine(self, quarantined_id: str, decision: str,
                           resolved_by: str, note: str = "") -> BatchRecord | None:
        """裁决隔离批次：accept 以隔离内容接替旧批次；reject 丢弃。"""
        q = self.state["batches"].get(quarantined_id)
        if q is None or q.status != "quarantined":
            raise NotFoundError(f"不存在待裁决的隔离批次：{quarantined_id}")
        if decision not in ("accept", "reject"):
            raise ValidationError("裁决结果必须是 accept 或 reject")
        if not note:
            raise ValidationError("裁决必须记录说明")
        if decision == "accept":
            new_id = self._unique_internal_id(f"{q.source_id}/{q.display_id}", "r")
            replacement = BatchRecord(
                batch_id=new_id, display_id=q.display_id, source_id=q.source_id,
                caliber_id=q.caliber_id, values=q.values, reported_unit=q.reported_unit,
                content_hash=q.content_hash, received_at=datetime.fromisoformat(now()),
                status="accepted", revision_of=q.conflicting_with,
                revision_reason=f"隔离裁决接受（{resolved_by}：{note}）",
            )
            caliber = self.state["calibers"][replacement.caliber_id]
            self._containment_precheck(caliber, replacement.values,
                                       exclude_batch_id=q.conflicting_with)
            self._emit("quarantine_resolved", {
                "quarantined_id": quarantined_id, "decision": "accept",
                "conflicting_with": q.conflicting_with, "replacement": replacement.to_dict(),
                "resolved_by": resolved_by, "note": note,
            })
            self._recompute_affected([caliber.caliber_id])
            return replacement
        self._emit("quarantine_resolved", {
            "quarantined_id": quarantined_id, "decision": "reject",
            "resolved_by": resolved_by, "note": note,
        })
        return None

    def _containment_precheck(self, target: Caliber, tentative_values: dict[str, float],
                              exclude_batch_id: str | None = None) -> None:
        """在写入事件前，试算受影响合计口径的包含关系。

        target 为合计口径时校验其拟接收值；target 为分项时校验所有父合计。
        父合计的分项尚未齐报时跳过，待齐或发布时再校验。
        """
        if target.kind == "aggregate":
            aggregates = [target]
        else:
            aggregates = [
                c for c in self.state["calibers"].values()
                if c.kind == "aggregate" and target.caliber_id in c.children
            ]
        for agg in aggregates:
            children = [self.state["calibers"][cid] for cid in agg.children]
            child_series = []
            ready = True
            for c in children:
                batches = [b for b in self._accepted_batches(c) if b.batch_id != exclude_batch_id]
                if c.caliber_id == target.caliber_id:
                    series = caliber_series(c, batches, self._approved_adjustments(c))
                    series.update(tentative_values)
                else:
                    if not batches:
                        ready = False
                    series = caliber_series(c, batches, self._approved_adjustments(c))
                child_series.append((c.name, series))
            if not ready:
                continue
            if agg.caliber_id == target.caliber_id:
                agg_batches = [b for b in self._accepted_batches(agg)
                               if b.batch_id != exclude_batch_id]
                reported = caliber_series(agg, agg_batches, self._approved_adjustments(agg))
                reported.update(tentative_values)
            else:
                ab = self._accepted_batches(agg)
                reported = (
                    caliber_series(agg, ab, self._approved_adjustments(agg)) if ab else None
                )
            check_containment(agg, child_series, reported)

    def _reject_before_accept(self, display_id: str, source_id: str, caliber_id: str,
                              values: dict[str, float], unit: str, chash: str,
                              reason: str) -> None:
        record = BatchRecord(
            batch_id=self._unique_internal_id(f"{source_id}/{display_id}", "b"),
            display_id=display_id, source_id=source_id, caliber_id=caliber_id,
            values=values, reported_unit=unit, content_hash=chash,
            received_at=datetime.fromisoformat(now()), status="rejected",
            resolution=reason,
        )
        self._emit("batch_rejected", {"batch": record.to_dict(), "reason": "containment"})

    # ================= 人工调整（双人复核） =================
    def propose_adjustment(self, adjustment_id: str, target: str, amount: float,
                           reason: str, created_by: str,
                           caliber_id: str | None = None, day: str | None = None) -> Adjustment:
        if not reason:
            raise ValidationError("人工调整必须说明原因")
        if target == "caliber":
            if not caliber_id or caliber_id not in self.state["calibers"]:
                raise ValidationError("口径级调整必须指定有效口径")
            if day is None:
                raise ValidationError("口径级调整必须指定具体日期")
            check_coverage({day: 0.0}, self._require_holiday())
        elif target != "national":
            raise ValidationError("调整目标必须是 caliber 或 national")
        adj = Adjustment(
            adjustment_id=adjustment_id, target=target, amount=float(amount), reason=reason,
            created_by=created_by, created_at=datetime.fromisoformat(now()),
            caliber_id=caliber_id, day=day,
        )
        self._emit("adjustment_proposed", adj.to_dict())
        return adj

    def review_adjustment(self, adjustment_id: str, reviewer: str,
                          approve: bool, note: str = "") -> Adjustment:
        adj = self.state["adjustments"].get(adjustment_id)
        if adj is None:
            raise NotFoundError(f"调整单不存在：{adjustment_id}")
        if adj.status != "pending":
            raise WorkflowError("该调整已完成复核，不可重复复核")
        if reviewer == adj.created_by:
            raise WorkflowError("人工调整必须由另一名人员复核，提交人不得复核自己的调整")
        if approve and adj.target == "caliber" and adj.day is not None:
            # 批准写入前预检：调整后不得破坏合计包含关系
            caliber = self.state["calibers"][adj.caliber_id]
            series = caliber_series(caliber, self._accepted_batches(caliber),
                                    self._approved_adjustments(caliber))
            tentative = dict(series)
            tentative[adj.day] = round(tentative.get(adj.day, 0.0) + adj.amount, 6)
            self._containment_precheck(caliber, tentative)
        self._emit("adjustment_reviewed", {
            "adjustment_id": adj.adjustment_id,
            "status": "approved" if approve else "rejected",
            "reviewed_by": reviewer, "reviewed_at": now(), "review_note": note,
        })
        if approve:
            if adj.target == "caliber":
                self._recompute_affected([adj.caliber_id])
            else:
                self._recompute_affected(list(self.state["calibers"]))
        return self.state["adjustments"][adjustment_id]

    # ================= 派生指标（增量重算） =================
    def _recompute_affected(self, changed_caliber_ids: list[str]) -> list[str]:
        """只重算输入指纹发生变化的派生指标，返回实际重算的指标键。"""
        holiday = self._require_holiday()
        changed_ids = set(changed_caliber_ids)
        affected_modes = {self.state["calibers"][cid].mode for cid in changed_ids}
        roots = [c for c in self.state["calibers"].values() if not self._is_child(c)]
        recomputed: list[str] = []

        for caliber in self.state["calibers"].values():
            affected = caliber.caliber_id in changed_ids or (
                caliber.kind == "aggregate"
                and any(ch in changed_ids for ch in caliber.children)
            )
            if affected:
                self._recompute_caliber(caliber, holiday, recomputed)

        for mode in sorted(affected_modes, key=lambda m: m.value):
            mode_roots = [c for c in roots if c.mode == mode]
            # 方式级指纹必须覆盖该方式全部口径（含合计的子分项），
            # 否则营业性客运等子项变化无法传播到方式合计与全国总量
            mode_all = [c for c in self.state["calibers"].values() if c.mode == mode]
            fp = fingerprint(mode=mode.value, inputs=[self._caliber_input_fp(c) for c in mode_all])
            if self._unchanged(f"mode:{mode.value}", fp):
                continue
            total = 0.0
            pending: list[str] = []
            for c in mode_roots:
                metric = self.state["derived"].get(f"caliber:{c.caliber_id}", {}).get("result")
                if metric is None:
                    pending.append(c.caliber_id)
                    continue
                total += metric["total"]
                if metric["pending"]:
                    pending.extend(metric["pending"])
            prior = sum(c.prior_year_total or 0.0 for c in mode_roots)
            self._store_derived(f"mode:{mode.value}", fp, {
                "total": round(total, 4),
                "daily_average": round(total / holiday["days"], 4),
                "growth": self._safe_growth(total, holiday["days"], prior),
                "pending": sorted(set(pending)),
                "unit": CANONICAL_UNIT,
            })
            recomputed.append(f"mode:{mode.value}")

        nat_adjs = [
            (a.adjustment_id, a.amount) for a in self.state["adjustments"].values()
            if a.status == "approved" and a.target == "national"
        ]
        fp = fingerprint(national=[self._caliber_input_fp(c) for c in roots],
                         national_adjustments=nat_adjs,
                         modes=[self.state["derived"].get(f"mode:{m.value}", {}).get("fingerprint")
                                for m in Mode])
        if not self._unchanged("national", fp):
            total = 0.0
            pending: list[str] = []
            for m in Mode:
                metric = self.state["derived"].get(f"mode:{m.value}", {}).get("result")
                if metric is None:
                    pending.extend(c.caliber_id for c in roots if c.mode == m)
                    continue
                total += metric["total"]
                pending.extend(metric["pending"])
            adj_total = sum(amount for _, amount in nat_adjs)
            total = round(total + adj_total, 4)
            prior = sum(c.prior_year_total or 0.0 for c in roots)
            self._store_derived("national", fp, {
                "total": total,
                "daily_average": round(total / holiday["days"], 4),
                "growth": self._safe_growth(total, holiday["days"], prior),
                "pending": sorted(set(pending)),
                "days": holiday["days"],
                "manual_adjustments": [
                    {"id": aid, "amount": amount} for aid, amount in nat_adjs
                ],
                "unit": CANONICAL_UNIT,
            })
            recomputed.append("national")
        return recomputed

    def _recompute_caliber(self, caliber: Caliber, holiday: dict, recomputed: list[str]) -> None:
        adjustments = self._approved_adjustments(caliber)
        if caliber.kind == "aggregate":
            children = [self.state["calibers"][cid] for cid in caliber.children]
            child_batches = {c.caliber_id: self._accepted_batches(c) for c in children}
            children_ready = all(batches for batches in child_batches.values())
            child_series = [
                (c.name, caliber_series(c, child_batches[c.caliber_id],
                                        self._approved_adjustments(c)))
                for c in children
            ]
            agg_batches = self._accepted_batches(caliber)
            reported = (
                caliber_series(caliber, agg_batches, adjustments) if agg_batches else None
            )
            if children_ready:
                # 分项齐备：无论合计是否直接报送，都校验包含关系
                series = check_containment(caliber, child_series, reported)
            elif reported is not None:
                # 分项未齐但合计已直接报送：暂用合计数，待分项齐备再校验
                series = reported
            else:
                # 双方都未齐：以已到分项做部分合成
                series = {
                    d: round(sum(s.get(d, 0.0) for _, s in child_series), 6)
                    for d in {d for _, s in child_series for d in s}
                }
            fp = fingerprint(
                caliber=caliber.caliber_id, aggregate=True,
                reported=reported, children_ready=children_ready,
                children=[self._caliber_input_fp(c) for c in children],
                adjustments=[(a["day"], a["amount"]) for a in adjustments],
            )
        else:
            series = caliber_series(caliber, self._accepted_batches(caliber), adjustments)
            fp = self._caliber_input_fp(caliber)

        all_days = self._holiday_days(holiday)
        missing_days = [d for d in all_days if d not in series]
        total = round(sum(series.values()), 4)
        if self._unchanged(f"caliber:{caliber.caliber_id}", fp):
            return
        self._store_derived(f"caliber:{caliber.caliber_id}", fp, {
            "total": total,
            "daily_average": round(total / holiday["days"], 4),
            "growth": self._safe_growth(
                total, holiday["days"],
                caliber.prior_year_total or 0.0,
                caliber.prior_year_days or holiday["days"],
            ),
            "incomparable_reason": caliber.incomparable_reason,
            "pending": [] if series and not missing_days else [caliber.caliber_id],
            "missing_days": missing_days,
            "series": series,
            "unit": CANONICAL_UNIT,
        })
        recomputed.append(f"caliber:{caliber.caliber_id}")

    def _safe_growth(self, total: float, days: int, prior: float, prior_days: int | None = None) -> float | None:
        if not prior or prior <= 0:
            return None
        try:
            return growth_rate(total, days, prior, prior_days or days)
        except ValidationError:
            return None

    def _unchanged(self, key: str, fp: str) -> bool:
        return self.state["derived"].get(key, {}).get("fingerprint") == fp

    def _store_derived(self, key: str, fp: str, result: dict) -> None:
        self._emit("derived_computed", {"key": key, "fingerprint": fp, "result": result})

    def _caliber_input_fp(self, caliber: Caliber) -> str:
        return fingerprint(
            caliber=caliber.caliber_id,
            batches=[(b.batch_id, b.content_hash) for b in self._accepted_batches(caliber)],
            adjustments=[(a.adjustment_id, a.amount, a.day)
                         for a in self.state["adjustments"].values()
                         if a.status == "approved" and a.target == "caliber"
                         and a.caliber_id == caliber.caliber_id],
        )

    # ================= 发布与勘误 =================
    def publish(self, version: str, publisher: str, title: str = "") -> dict:
        self._require_holiday()
        if any(v["version"] == version for v in self.state["publications"]):
            raise WorkflowError(f"发布版本号已存在：{version}")
        self._force_full_validation()
        snapshot = self._build_version(version, publisher, title, kind="published", supersedes=None)
        self._emit("published", {"version": snapshot})
        return snapshot

    def issue_corrigendum(self, new_version: str, publisher: str, reason: str,
                          title: str = "") -> dict:
        """已发布版本永不原地覆盖；修订以勘误形式衔接为新版本。"""
        if not reason:
            raise ValidationError("勘误必须说明原因")
        if not self.state["publications"]:
            raise WorkflowError("尚无已发布版本，无法勘误")
        if any(v["version"] == new_version for v in self.state["publications"]):
            raise WorkflowError(f"发布版本号已存在：{new_version}")
        self._force_full_validation()
        prior = self.state["publications"][-1]["version"]
        snapshot = self._build_version(new_version, publisher, title,
                                       kind="corrigendum", supersedes=prior, reason=reason)
        self._emit("corrigendum_issued", {"version": snapshot})
        return snapshot

    def _force_full_validation(self) -> None:
        """发布前对分项已齐的合计口径强制做一次包含关系校验。

        分项未齐的合计不在这里强判（最后一个分项到达时预检必然校验），
        其缺口经版本快照的 pending 清单对外披露。
        """
        for caliber in self.state["calibers"].values():
            if caliber.kind != "aggregate":
                continue
            children = [self.state["calibers"][cid] for cid in caliber.children]
            if any(not self._accepted_batches(c) for c in children):
                continue
            child_series = [
                (c.name, caliber_series(c, self._accepted_batches(c), self._approved_adjustments(c)))
                for c in children
            ]
            reported = (
                caliber_series(caliber, self._accepted_batches(caliber),
                               self._approved_adjustments(caliber))
                if self._accepted_batches(caliber) else None
            )
            check_containment(caliber, child_series, reported)
        self._recompute_affected(list(self.state["calibers"]))

    def _build_version(self, version: str, publisher: str, title: str,
                       kind: str, supersedes: str | None, reason: str | None = None) -> dict:
        derived = self.state["derived"]
        national = derived.get("national", {}).get("result")
        modes = {}
        for m in Mode:
            r = derived.get(f"mode:{m.value}", {}).get("result")
            if r is not None:
                modes[m.value] = {**r, "name": MODE_LABELS[m]}
        calibers = {
            cid: derived[f"caliber:{cid}"]["result"]
            for cid in self.state["calibers"] if f"caliber:{cid}" in derived
        }
        batch_refs = sorted(b.batch_id for b in self.state["batches"].values() if b.status == "accepted")
        adj_refs = sorted(a.adjustment_id for a in self.state["adjustments"].values()
                          if a.status == "approved")
        return {
            "version": version, "title": title, "kind": kind,
            "supersedes": supersedes, "corrigendum_reason": reason,
            "published_at": now(), "published_by": publisher,
            "holiday": self.state["holiday"],
            "national": national, "modes": modes, "calibers": calibers,
            "batch_refs": batch_refs, "adjustment_refs": adj_refs,
            "pending": self._pending_items(),
            "quarantined": [
                {"id": b.batch_id, "display_id": b.display_id, "caliber_id": b.caliber_id}
                for b in self.state["quarantine"].values()
            ],
        }

    def explain_version(self, version: str) -> dict[str, Any]:
        """按任一历史版本解释：采用哪些批次、哪些仍待确认、与早先公报为何不同。"""
        snapshots = {v["version"]: v for v in self.state["publications"]}
        snap = snapshots.get(version)
        if snap is None:
            raise NotFoundError(f"历史版本不存在：{version}")
        batches = []
        for bid in snap["batch_refs"]:
            b = self.state["batches"].get(bid)
            if b is None:
                continue
            batches.append({
                "batch_id": b.batch_id, "display_id": b.display_id,
                "source": self.state["sources"][b.source_id].name,
                "caliber": self.state["calibers"][b.caliber_id].name,
                "revision_of": b.revision_of, "revision_reason": b.revision_reason,
                "received_at": b.received_at.isoformat(),
            })
        adjustments = []
        for aid in snap["adjustment_refs"]:
            a = self.state["adjustments"][aid]
            adjustments.append({
                "adjustment_id": a.adjustment_id, "target": a.target,
                "amount": a.amount, "reason": a.reason,
                "created_by": a.created_by, "reviewed_by": a.reviewed_by,
            })
        explanation: dict[str, Any] = {
            "version": version,
            "kind": snap["kind"],
            "national_total": (snap["national"] or {}).get("total"),
            "unit": CANONICAL_UNIT,
            "batches_used": batches,
            "adjustments_applied": adjustments,
            "pending": snap["pending"],
            "quarantined_at_publish": snap["quarantined"],
        }
        if snap["supersedes"]:
            explanation["diff_against"] = self._diff_versions(snapshots[snap["supersedes"]], snap)
        return explanation

    def _diff_versions(self, prior: dict, current: dict) -> dict:
        changes: list[dict] = []
        p_nat, c_nat = prior.get("national") or {}, current.get("national") or {}
        if p_nat.get("total") != c_nat.get("total"):
            changes.append({
                "scope": "national",
                "from": p_nat.get("total"), "to": c_nat.get("total"),
                "delta": round((c_nat.get("total") or 0) - (p_nat.get("total") or 0), 4),
            })
        for mid, cur_m in current["modes"].items():
            pre_m = prior["modes"].get(mid, {})
            if pre_m.get("total") != cur_m["total"]:
                changes.append({
                    "scope": f"mode:{mid}", "name": cur_m.get("name"),
                    "from": pre_m.get("total"), "to": cur_m["total"],
                    "delta": round((cur_m["total"] or 0) - (pre_m.get("total") or 0), 4),
                })
        prior_batches, cur_batches = set(prior["batch_refs"]), set(current["batch_refs"])
        batch_changes = []
        for bid in sorted(cur_batches - prior_batches):
            b = self.state["batches"][bid]
            batch_changes.append({
                "batch_id": bid, "display_id": b.display_id,
                "action": "revision" if b.revision_of else "added",
                "reason": b.revision_reason,
            })
        for bid in sorted(prior_batches - cur_batches):
            b = self.state["batches"][bid]
            batch_changes.append({
                "batch_id": bid, "display_id": b.display_id,
                "action": "removed",
                "reason": b.resolution or "被修订批次接替",
            })
        return {
            "prior_version": prior["version"],
            "corrigendum_reason": current.get("corrigendum_reason"),
            "metric_changes": changes,
            "batch_changes": batch_changes,
            "adjustments_added": sorted(set(current["adjustment_refs"]) - set(prior["adjustment_refs"])),
        }

    # ================= 待发布任务与截止期提醒（恢复续跑） =================
    def create_publish_task(self, task_id: str, title: str, due: str, created_by: str) -> PublishTask:
        if task_id in self.state["tasks"]:
            raise WorkflowError(f"任务已存在：{task_id}")
        task = PublishTask(
            task_id=task_id, title=title, due=datetime.fromisoformat(due),
            created_by=created_by, created_at=datetime.fromisoformat(now()),
        )
        self._emit("task_created", task.to_dict())
        return task

    def complete_task(self, task_id: str) -> None:
        task = self.state["tasks"].get(task_id)
        if task is None:
            raise NotFoundError(f"任务不存在：{task_id}")
        if task.status != "open":
            raise WorkflowError("任务已关闭")
        self._emit("task_completed", {"task_id": task_id, "completed_at": now()})

    def pending_reminders(self, at: str | None = None, within_hours: float = 24.0) -> list[dict]:
        """口径截止期临期/逾期未报齐，或待发布任务临期/逾期。重启后仍可继续。"""
        moment = datetime.fromisoformat(at or now())
        window = timedelta(hours=within_hours)
        out: list[dict] = []
        holiday = self.state["holiday"]
        expected_days = self._holiday_days(holiday) if holiday else []
        for caliber in self.state["calibers"].values():
            covered = self._covered_days(caliber)
            missing = [d for d in expected_days if d not in covered]
            if not missing or caliber.deadline is None:
                continue
            if caliber.deadline < moment or moment <= caliber.deadline <= moment + window:
                out.append({
                    "key": f"caliber:{caliber.caliber_id}", "kind": "deadline",
                    "caliber_id": caliber.caliber_id, "name": caliber.name,
                    "deadline": caliber.deadline.isoformat(),
                    "missing_days": missing,
                    "overdue": caliber.deadline < moment,
                })
        for task in self.state["tasks"].values():
            if task.status != "open":
                continue
            if task.due < moment or moment <= task.due <= moment + window:
                out.append({
                    "key": f"task:{task.task_id}", "kind": "publish_task",
                    "task_id": task.task_id, "title": task.title,
                    "due": task.due.isoformat(), "overdue": task.due < moment,
                })
        return out

    def fire_due_reminders(self, at: str | None = None, within_hours: float = 24.0) -> list[dict]:
        """触发提醒并留痕；缺失集合指纹纳入去重键，数据补齐/变化后可再次提醒。"""
        fired = []
        for item in self.pending_reminders(at, within_hours):
            dedupe_key = item["key"] + ":" + fingerprint(
                missing=item.get("missing_days"), overdue=item.get("overdue"),
            )
            if dedupe_key in self.state["reminders"]:
                continue
            self._emit("reminder_fired", {"key": dedupe_key, "item": item})
            fired.append(item)
        return fired

    def resume(self, at: str | None = None) -> dict:
        """服务恢复入口：落快照加速下次启动，并继续未完成任务与截止期提醒。"""
        self.checkpoint()
        return {
            "open_tasks": [t.to_dict() for t in self.state["tasks"].values() if t.status == "open"],
            "quarantine": [
                {"id": b.batch_id, "display_id": b.display_id, "caliber_id": b.caliber_id}
                for b in self.state["quarantine"].values()
            ],
            "pending_adjustments": [
                a.adjustment_id for a in self.state["adjustments"].values() if a.status == "pending"
            ],
            "reminders_due": self.pending_reminders(at),
            "publications": [v["version"] for v in self.state["publications"]],
            "last_seq": self.state["_last_seq"],
        }

    def checkpoint(self) -> None:
        self.store.save_snapshot(to_jsonable(self.state))

    # ================= 查询辅助 =================
    def get_batch(self, batch_id: str) -> BatchRecord:
        b = self.state["batches"].get(batch_id)
        if b is None:
            raise NotFoundError(f"批次不存在：{batch_id}")
        return b

    def metrics(self) -> dict:
        return {k: v["result"] for k, v in self.state["derived"].items()}

    def _pending_items(self) -> list[dict]:
        holiday = self.state["holiday"]
        days = self._holiday_days(holiday)
        items: list[dict] = []
        for caliber in self.state["calibers"].values():
            metric = self.state["derived"].get(f"caliber:{caliber.caliber_id}", {}).get("result")
            missing = metric["missing_days"] if metric else list(days)
            if caliber.kind == "aggregate":
                children = [self.state["calibers"][cid] for cid in caliber.children]
                child_missing = []
                for c in children:
                    cm = self.state["derived"].get(f"caliber:{c.caliber_id}", {}).get("result")
                    child_missing += cm["missing_days"] if cm else list(days)
                child_missing = sorted(set(child_missing))
                if not self._accepted_batches(caliber) and child_missing:
                    items.append({"caliber_id": caliber.caliber_id, "name": caliber.name,
                                  "missing_days": child_missing,
                                  "reason": "合计未直接报送且分项未齐"})
                elif child_missing:
                    items.append({"caliber_id": caliber.caliber_id, "name": caliber.name,
                                  "missing_days": child_missing, "reason": "分项数据待确认"})
                elif missing:
                    items.append({"caliber_id": caliber.caliber_id, "name": caliber.name,
                                  "missing_days": missing, "reason": "覆盖日期未齐"})
            elif missing:
                items.append({"caliber_id": caliber.caliber_id, "name": caliber.name,
                              "missing_days": missing, "reason": "来源尚未报齐"})
        return items

    # ================= 内部工具 =================
    def _accepted_batches(self, caliber: Caliber) -> list[BatchRecord]:
        return [b for b in self.state["batches"].values()
                if b.caliber_id == caliber.caliber_id and b.status == "accepted"]

    def _covered_days(self, caliber: Caliber) -> set[str]:
        """该口径已有确定值的日期：合计口径可由分项合成覆盖。"""
        days = {d for b in self._accepted_batches(caliber) for d in b.values}
        days.update(a.day for a in self._approved_adjustments(caliber) if a.get("day"))
        if caliber.kind == "aggregate":
            children = [self.state["calibers"][cid] for cid in caliber.children]
            if children:
                child_cover = [self._covered_days(c) for c in children]
                # 所有分项都覆盖的日期，合计才可由分项确定
                days.update(set.intersection(*child_cover) if child_cover else set())
        return days

    def _approved_adjustments(self, caliber: Caliber) -> list[dict[str, Any]]:
        return [
            {"day": a.day, "amount": a.amount}
            for a in self.state["adjustments"].values()
            if a.status == "approved" and a.target == "caliber" and a.caliber_id == caliber.caliber_id
        ]

    def _is_child(self, caliber: Caliber) -> bool:
        return any(caliber.caliber_id in c.children for c in self.state["calibers"].values())

    def _holiday_days(self, holiday: dict) -> list[str]:
        from datetime import date as _date
        d0 = _date.fromisoformat(holiday["start"])
        d1 = _date.fromisoformat(holiday["end"])
        out, cur = [], d0
        while cur <= d1:
            out.append(cur.isoformat())
            cur += timedelta(days=1)
        return out

    def _unique_internal_id(self, stem: str, tag: str) -> str:
        n = 1
        existing = set(self.state["batches"])
        while True:
            candidate = f"{stem}#{tag}{n}"
            if candidate not in existing:
                return candidate
            n += 1

    def _require_holiday(self) -> dict:
        if not self.state["holiday"]:
            raise WorkflowError("尚未开启假期归集窗口")
        return self.state["holiday"]

    def _emit(self, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        event = self.store.append(event_type, payload, now())
        apply_event(self.state, event)
        return event
