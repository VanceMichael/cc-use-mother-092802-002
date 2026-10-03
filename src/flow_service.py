"""客流归集服务门面。

把报送受理（去重、隔离、修订）、派生指标增量重算、人工调整双人复核、
发布与勘误、历史版本解释、截止期提醒与恢复串联为一个服务。全部状态经
Store 持久化，服务重启后用同一 Store 重新构造即可恢复。
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Iterable

from .flow_caliber import (
    DEFAULT_CALIBER_ID,
    DEFAULT_TREE,
    Caliber,
    CaliberRegistry,
    validate_batch,
)
from .flow_metrics import compute_mode_aggregate, compute_period_metrics
from .flow_models import (
    ADJ_REJECTED,
    APPROVED,
    DONE,
    INCLUDED,
    OPEN,
    PENDING,
    PUBLISH_TASK,
    REMINDER,
    SUPERSEDED,
    Adjustment,
    Batch,
    Explanation,
    IngestResult,
    ModeAggregate,
    PeriodMetrics,
    PublishedVersion,
    QuarantineEntry,
    Task,
)
from .flow_store import Store


class FlowAggregationService:
    """跨方式假期客流归集服务。"""

    def __init__(self, store: Store) -> None:
        self._store = store
        self._registry = CaliberRegistry()
        self._periods: dict[str, dict[str, Any]] = {}
        self._batches: dict[tuple[str, str], Batch] = {}
        self._quarantine: dict[str, QuarantineEntry] = {}
        self._adjustments: dict[str, Adjustment] = {}
        self._versions: list[PublishedVersion] = []
        self._tasks: dict[str, Task] = {}
        self._cache: dict[tuple[str, str], ModeAggregate] = {}
        self._recompute_log: list[dict[str, Any]] = []
        self._load()
        if not self._registry.all():
            self._registry.register(Caliber(DEFAULT_CALIBER_ID, 1, DEFAULT_TREE))
            self._save_calibers()

    # ------------------------------------------------------------------
    # 持久化

    def _load(self) -> None:
        for raw in self._store.load("calibers") or []:
            self._registry.register(Caliber.from_dict(raw))
        for raw in self._store.load("periods") or []:
            self._periods[raw["period_id"]] = raw
        for raw in self._store.load("batches") or []:
            batch = Batch.from_dict(raw)
            self._batches[(batch.source, batch.batch_id)] = batch
        for raw in self._store.load("quarantine") or []:
            entry = QuarantineEntry.from_dict(raw)
            self._quarantine[entry.quarantine_id] = entry
        for raw in self._store.load("adjustments") or []:
            adjustment = Adjustment.from_dict(raw)
            self._adjustments[adjustment.adjustment_id] = adjustment
        for raw in self._store.load("versions") or []:
            self._versions.append(PublishedVersion.from_dict(raw))
        for raw in self._store.load("tasks") or []:
            task = Task.from_dict(raw)
            self._tasks[task.task_id] = task
        for key, raw in (self._store.load("metric_cache") or {}).items():
            period_id, mode = key.split("|", 1)
            self._cache[(period_id, mode)] = ModeAggregate.from_dict(raw)
        self._recompute_log = list(self._store.load("recompute_log") or [])

    def _save_calibers(self) -> None:
        self._store.save("calibers", [c.to_dict() for c in self._registry.all()])

    def _save_periods(self) -> None:
        self._store.save("periods", list(self._periods.values()))

    def _save_batches(self) -> None:
        self._store.save("batches", [b.to_dict() for b in self._batches.values()])

    def _save_quarantine(self) -> None:
        self._store.save("quarantine", [q.to_dict() for q in self._quarantine.values()])

    def _save_adjustments(self) -> None:
        self._store.save("adjustments", [a.to_dict() for a in self._adjustments.values()])

    def _save_versions(self) -> None:
        self._store.save("versions", [v.to_dict() for v in self._versions])

    def _save_tasks(self) -> None:
        self._store.save("tasks", [t.to_dict() for t in self._tasks.values()])

    def _save_cache(self) -> None:
        self._store.save(
            "metric_cache",
            {f"{p}|{m}": a.to_dict() for (p, m), a in self._cache.items()},
        )
        self._store.save("recompute_log", self._recompute_log)

    # ------------------------------------------------------------------
    # 基础登记

    @staticmethod
    def _ts(at: datetime | None) -> str:
        return (at or datetime.now(timezone.utc)).isoformat()

    def register_caliber(self, caliber: Caliber) -> None:
        self._registry.register(caliber)
        self._save_calibers()

    def register_period(
        self,
        period_id: str,
        start: date,
        end: date,
        compare_to: str | None = None,
    ) -> None:
        """登记假期及其覆盖日期，compare_to 指向可比增幅的参照期。"""
        if end < start:
            raise ValueError("假期起止日期颠倒")
        self._periods[period_id] = {
            "period_id": period_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "compare_to": compare_to,
        }
        self._save_periods()

    def _period(self, period_id: str) -> dict[str, Any]:
        period = self._periods.get(period_id)
        if period is None:
            raise ValueError(f"假期{period_id}未登记")
        return period

    def _period_days(self, period_id: str) -> int:
        period = self._period(period_id)
        return (date.fromisoformat(period["end"]) - date.fromisoformat(period["start"])).days + 1

    def _leaf_modes(self) -> tuple[str, ...]:
        caliber = self._registry.latest(DEFAULT_CALIBER_ID)
        assert caliber is not None
        return caliber.leaf_modes()

    # ------------------------------------------------------------------
    # 报送受理：去重、隔离、修订

    def submit_batch(self, batch: Batch, at: datetime | None = None) -> IngestResult:
        """受理一个原始批次。

        同一来源重复报送内容相同的批次直接认定为重复，不重复累计；
        编号相同而内容不同的批次进入隔离区；修订批次取代原批次并只
        重算受影响的分项。
        """
        now = self._ts(at)
        batch.received_at = now
        key = (batch.source, batch.batch_id)
        existing = self._batches.get(key)
        if existing is not None:
            if existing.status == INCLUDED and existing.fingerprint() == batch.fingerprint():
                return IngestResult("duplicate", existing)
            reason = "编号相同但内容不同，先隔离待处理"
            if existing.status == SUPERSEDED and existing.fingerprint() == batch.fingerprint():
                reason = "该批次内容已被修订取代，先隔离待处理"
            qid = self._open_quarantine(batch, (reason,), now)
            return IngestResult("quarantined", batch, (reason,), qid)

        issues: list[str] = []
        period = self._periods.get(batch.period_id)
        if period is None:
            issues.append(f"假期{batch.period_id}未登记")
        caliber = self._registry.get(batch.caliber_id, batch.caliber_version)
        if caliber is None:
            issues.append(f"口径{batch.caliber_id}版本{batch.caliber_version}未注册")
        target: Batch | None = None
        if batch.revises:
            target = self._batches.get((batch.source, batch.revises))
            if target is None or target.status != INCLUDED:
                issues.append(f"修订目标批次{batch.revises}不存在或已失效")
            if not batch.revision_reason:
                issues.append("修订批次必须填写修订原因")
        if period is not None and caliber is not None:
            issues.extend(
                validate_batch(
                    batch,
                    caliber,
                    date.fromisoformat(period["start"]),
                    date.fromisoformat(period["end"]),
                )
            )
        if issues:
            qid = self._open_quarantine(batch, tuple(issues), now)
            return IngestResult("quarantined", batch, tuple(issues), qid)

        if target is not None:
            target.status = SUPERSEDED
        batch.status = INCLUDED
        self._batches[key] = batch
        affected = self._affected_leaf_modes(batch)
        self._recompute(batch.period_id, affected, f"批次{batch.batch_id}纳入汇总", now)
        self._fulfill_tasks(batch)
        self._save_batches()
        self._save_tasks()
        status = "revised" if batch.revises else "included"
        return IngestResult(status, batch, affected_modes=tuple(sorted(affected)))

    def _open_quarantine(self, batch: Batch, reasons: tuple[str, ...], now: str) -> str:
        seq = len(self._quarantine) + 1
        quarantine_id = f"Q{seq}"
        while quarantine_id in self._quarantine:
            seq += 1
            quarantine_id = f"Q{seq}"
        self._quarantine[quarantine_id] = QuarantineEntry(
            quarantine_id=quarantine_id, batch=batch, reasons=reasons, opened_at=now
        )
        self._save_quarantine()
        return quarantine_id

    def resolve_quarantine(
        self,
        quarantine_id: str,
        action: str,
        actor: str,
        reason: str = "",
        at: datetime | None = None,
    ) -> IngestResult:
        """处理隔离批次：accept_revision 作为修订纳入，reject 予以退回。"""
        entry = self._quarantine.get(quarantine_id)
        if entry is None:
            raise ValueError(f"隔离记录{quarantine_id}不存在")
        if entry.status != OPEN:
            raise ValueError("隔离记录已处理")
        now = self._ts(at)
        if action == "reject":
            entry.status = DONE
            entry.resolution = "reject"
            entry.resolved_by = actor
            entry.resolved_at = now
            self._save_quarantine()
            return IngestResult("rejected", entry.batch, entry.reasons, quarantine_id)
        if action != "accept_revision":
            raise ValueError(f"未知处理方式{action}")
        batch = entry.batch
        existing = self._batches.get((batch.source, batch.batch_id))
        if existing is None or existing.status != INCLUDED:
            raise ValueError("隔离批次没有可取代的现行批次")
        if not reason:
            raise ValueError("按修订纳入时必须填写修订原因")
        batch.revises = batch.batch_id
        batch.revision_reason = reason
        existing.status = SUPERSEDED
        batch.status = INCLUDED
        self._batches[(batch.source, batch.batch_id)] = batch
        entry.status = DONE
        entry.resolution = "accept_revision"
        entry.resolved_by = actor
        entry.resolved_at = now
        affected = self._affected_leaf_modes(batch)
        self._recompute(batch.period_id, affected, f"隔离批次{batch.batch_id}按修订纳入", now)
        self._fulfill_tasks(batch)
        self._save_batches()
        self._save_quarantine()
        self._save_tasks()
        return IngestResult("revised", batch, affected_modes=tuple(sorted(affected)))

    def _affected_leaf_modes(self, batch: Batch) -> set[str]:
        caliber = self._registry.get(batch.caliber_id, batch.caliber_version)
        if caliber is None:
            return set()
        affected: set[str] = set()
        for mode in batch.modes_touched():
            leaf = caliber.leaf_under_total(mode)
            if leaf == "total":
                affected.update(caliber.leaf_modes())
            elif leaf is not None:
                affected.add(leaf)
        return affected

    # ------------------------------------------------------------------
    # 派生指标：增量重算

    def _recompute(self, period_id: str, modes: set[str], reason: str, now: str) -> None:
        """只重算受影响的分项，未受影响的分项沿用缓存。"""
        if not modes:
            return
        days = self._period_days(period_id)
        batches = [
            b for b in self._batches.values() if b.period_id == period_id and b.status == INCLUDED
        ]
        adjustments = list(self._adjustments.values())
        changed = []
        for mode in sorted(modes):
            self._cache[(period_id, mode)] = compute_mode_aggregate(
                period_id, days, mode, batches, adjustments, self._registry
            )
            changed.append(mode)
        self._recompute_log.append(
            {"at": now, "period_id": period_id, "modes": changed, "reason": reason}
        )
        self._save_cache()

    def metrics(self, period_id: str, at: datetime | None = None) -> PeriodMetrics:
        """当前派生指标：分方式汇总、假期总量、日均与可比增幅。"""
        self._period(period_id)
        missing = [m for m in self._leaf_modes() if (period_id, m) not in self._cache]
        if missing:
            self._recompute(period_id, set(missing), "初始计算", self._ts(at))
        aggregates = {m: self._cache[(period_id, m)] for m in self._leaf_modes()}
        reference_id = self._period(period_id).get("compare_to")
        reference_aggregates = None
        if reference_id and reference_id in self._periods:
            missing_ref = [
                m for m in self._leaf_modes() if (reference_id, m) not in self._cache
            ]
            if missing_ref:
                self._recompute(reference_id, set(missing_ref), "初始计算", self._ts(at))
            reference_aggregates = {m: self._cache[(reference_id, m)] for m in self._leaf_modes()}
        return compute_period_metrics(
            period_id,
            self._period_days(period_id),
            aggregates,
            reference_id if reference_aggregates else None,
            reference_aggregates,
        )

    def recompute_log(self) -> list[dict[str, Any]]:
        """增量重算轨迹，用于核对哪些派生指标被重算过。"""
        return list(self._recompute_log)

    # ------------------------------------------------------------------
    # 人工调整：提交与复核必须不同人

    def propose_adjustment(
        self,
        period_id: str,
        mode: str,
        delta: Decimal,
        reason: str,
        proposed_by: str,
        at: datetime | None = None,
    ) -> Adjustment:
        """提交人工调整，复核通过前不参与汇总。"""
        self._period(period_id)
        if mode not in self._leaf_modes():
            raise ValueError(f"分项{mode}不是可调整的汇总分项")
        if not reason.strip():
            raise ValueError("人工调整必须填写原因")
        if not proposed_by.strip():
            raise ValueError("人工调整必须登记提交人")
        seq = len(self._adjustments) + 1
        adjustment_id = f"ADJ{seq}"
        while adjustment_id in self._adjustments:
            seq += 1
            adjustment_id = f"ADJ{seq}"
        adjustment = Adjustment(
            adjustment_id=adjustment_id,
            period_id=period_id,
            mode=mode,
            delta=delta,
            reason=reason,
            proposed_by=proposed_by,
            proposed_at=self._ts(at),
        )
        self._adjustments[adjustment_id] = adjustment
        self._save_adjustments()
        return adjustment

    def review_adjustment(
        self,
        adjustment_id: str,
        reviewer: str,
        approve: bool,
        at: datetime | None = None,
    ) -> Adjustment:
        """复核人工调整；复核人不得与提交人相同。"""
        adjustment = self._adjustments.get(adjustment_id)
        if adjustment is None:
            raise ValueError(f"调整{adjustment_id}不存在")
        if adjustment.status != PENDING:
            raise ValueError("调整已复核，不能重复处理")
        if reviewer == adjustment.proposed_by:
            raise ValueError("复核人不能与提交人相同")
        now = self._ts(at)
        adjustment.status = APPROVED if approve else ADJ_REJECTED
        adjustment.reviewed_by = reviewer
        adjustment.reviewed_at = now
        if approve:
            self._recompute(
                adjustment.period_id,
                {adjustment.mode},
                f"人工调整{adjustment.adjustment_id}复核通过",
                now,
            )
        self._save_adjustments()
        return adjustment

    # ------------------------------------------------------------------
    # 发布与勘误

    def publish(
        self,
        period_id: str,
        note: str = "",
        at: datetime | None = None,
        errata_of: str | None = None,
        errata_reason: str | None = None,
    ) -> PublishedVersion:
        """发布当前汇总结果为一个不可变版本。"""
        now = self._ts(at)
        current = self.metrics(period_id, at)
        sequence = 1 + max(
            (v.sequence for v in self._versions if v.period_id == period_id), default=0
        )
        version = PublishedVersion(
            version_id=f"{period_id}-v{sequence}",
            period_id=period_id,
            sequence=sequence,
            published_at=now,
            metrics=current,
            lineage={
                mode: {
                    "batches": list(aggregate.batches),
                    "adjustment_ids": list(aggregate.adjustment_ids),
                    "caliber_id": aggregate.caliber_id,
                    "caliber_version": aggregate.caliber_version,
                }
                for mode, aggregate in current.modes.items()
            },
            pending=self._pending_items(period_id),
            note=note,
            errata_of=errata_of,
            errata_reason=errata_reason,
        )
        self._versions.append(version)
        for task in self._tasks.values():
            if task.kind == PUBLISH_TASK and task.period_id == period_id and task.status == OPEN:
                task.status = DONE
        self._save_versions()
        self._save_tasks()
        return version

    def publish_errata(
        self, period_id: str, reason: str, at: datetime | None = None
    ) -> PublishedVersion:
        """对已发布结果出具勘误：生成新版本并链接原版本，原版本不被覆盖。"""
        previous = self.latest_version(period_id)
        if previous is None:
            raise ValueError("尚无已发布版本，无法勘误")
        if not reason.strip():
            raise ValueError("勘误必须填写原因")
        return self.publish(
            period_id,
            note=f"勘误：{reason}",
            at=at,
            errata_of=previous.version_id,
            errata_reason=reason,
        )

    def latest_version(self, period_id: str) -> PublishedVersion | None:
        versions = [v for v in self._versions if v.period_id == period_id]
        return max(versions, key=lambda v: v.sequence, default=None)

    def versions(self, period_id: str) -> list[PublishedVersion]:
        return sorted(
            (v for v in self._versions if v.period_id == period_id), key=lambda v: v.sequence
        )

    def pending_items(self, period_id: str) -> tuple[str, ...]:
        """当前仍待确认的数据项（隔离批次、未报送来源、待复核调整）。"""
        return self._pending_items(period_id)

    def _pending_items(self, period_id: str) -> tuple[str, ...]:
        items: list[str] = []
        for entry in self._quarantine.values():
            if entry.status == OPEN and entry.batch.period_id == period_id:
                items.append(
                    f"批次{entry.batch.batch_id}（{entry.batch.source}）隔离待处理：{entry.reasons[0]}"
                )
        for task in self._tasks.values():
            if task.kind == REMINDER and task.period_id == period_id and task.status == OPEN:
                items.append(f"{task.source}的{task.mode}数据待报送（截止{task.due.isoformat()}）")
        for adjustment in self._adjustments.values():
            if adjustment.period_id == period_id and adjustment.status == PENDING:
                items.append(f"人工调整{adjustment.adjustment_id}待复核")
        return tuple(items)

    # ------------------------------------------------------------------
    # 历史版本解释

    def explain(self, version_id: str) -> Explanation:
        """解释任一历史版本：采用的批次、待确认数据、与前一版的差异。"""
        version = next((v for v in self._versions if v.version_id == version_id), None)
        if version is None:
            raise ValueError(f"发布版本{version_id}不存在")
        batches_used: list[dict[str, Any]] = []
        seen: set[str] = set()
        adjustment_ids: list[str] = []
        for mode_lineage in version.lineage.values():
            for ref in mode_lineage["batches"]:
                if ref["batch_id"] not in seen:
                    seen.add(ref["batch_id"])
                    batches_used.append(dict(ref))
            adjustment_ids.extend(mode_lineage["adjustment_ids"])
        adjustments = tuple(
            self._adjustments[aid].to_dict()
            for aid in dict.fromkeys(adjustment_ids)
            if aid in self._adjustments
        )
        previous = next(
            (
                v
                for v in self._versions
                if v.period_id == version.period_id and v.sequence == version.sequence - 1
            ),
            None,
        )
        differences = self._diff_versions(previous, version) if previous else ()
        return Explanation(
            version_id=version.version_id,
            period_id=version.period_id,
            published_at=version.published_at,
            batches_used=tuple(batches_used),
            pending=version.pending,
            adjustments=adjustments,
            differs_from=previous.version_id if previous else None,
            differences=differences,
            errata_of=version.errata_of,
            errata_reason=version.errata_reason,
        )

    def _diff_versions(
        self, old: PublishedVersion, new: PublishedVersion
    ) -> tuple[str, ...]:
        messages: list[str] = []
        for mode, new_agg in new.metrics.modes.items():
            old_agg = old.metrics.modes.get(mode)
            if old_agg is None or old_agg.total == new_agg.total:
                continue
            causes: list[str] = []
            old_refs = {r["batch_id"]: r for r in old_agg.batches}
            new_refs = {r["batch_id"]: r for r in new_agg.batches}
            for batch_id, ref in new_refs.items():
                if batch_id not in old_refs:
                    note = f"（修订：{ref['revision_reason']}）" if ref.get("revision_reason") else ""
                    causes.append(f"新增批次{batch_id}{note}")
                elif old_refs[batch_id]["fingerprint"] != ref["fingerprint"]:
                    reason = ref.get("revision_reason") or "未注明原因"
                    causes.append(f"批次{batch_id}内容修订（{reason}）")
            for batch_id in old_refs:
                if batch_id not in new_refs:
                    causes.append(f"批次{batch_id}不再采用")
            for aid in sorted(set(new_agg.adjustment_ids) - set(old_agg.adjustment_ids)):
                adjustment = self._adjustments.get(aid)
                if adjustment is not None:
                    causes.append(
                        f"人工调整{adjustment.delta:+}万人次"
                        f"（{adjustment.reason}，复核人{adjustment.reviewed_by}）"
                    )
            cause_text = "；".join(causes) if causes else "口径或覆盖范围变化"
            messages.append(
                f"{mode}：{old_agg.total} → {new_agg.total}万人次（{cause_text}）"
            )
        if old.metrics.holiday_total != new.metrics.holiday_total:
            messages.append(
                f"假期总量：{old.metrics.holiday_total} → {new.metrics.holiday_total}万人次"
            )
        return tuple(messages)

    # ------------------------------------------------------------------
    # 截止期提醒与待发布任务

    def plan_period(
        self,
        period_id: str,
        expectations: Iterable[dict[str, Any]],
        publish_deadline: date | None = None,
    ) -> None:
        """为假期登记各来源的报送截止期与发布截止任务。"""
        self._period(period_id)
        for expectation in expectations:
            source = expectation["source"]
            mode = expectation["mode"]
            due = expectation["due"]
            task_id = f"remind:{period_id}:{source}:{mode}"
            task = Task(
                task_id=task_id,
                kind=REMINDER,
                period_id=period_id,
                due=due,
                message=f"{source}的{mode}数据已过截止期{due.isoformat()}仍未报送",
                source=source,
                mode=mode,
            )
            if self._expectation_satisfied(period_id, source, mode):
                task.status = DONE
            self._tasks[task_id] = task
        if publish_deadline is not None:
            task = Task(
                task_id=f"publish:{period_id}",
                kind=PUBLISH_TASK,
                period_id=period_id,
                due=publish_deadline,
                message=f"假期{period_id}汇总结果待发布",
            )
            if self.latest_version(period_id) is not None:
                task.status = DONE
            self._tasks[task.task_id] = task
        self._save_tasks()

    def _expectation_satisfied(self, period_id: str, source: str, mode: str) -> bool:
        for batch in self._batches.values():
            if (
                batch.period_id == period_id
                and batch.source == source
                and batch.status == INCLUDED
                and mode in self._affected_leaf_modes(batch)
            ):
                return True
        return False

    def _fulfill_tasks(self, batch: Batch) -> None:
        if batch.status != INCLUDED:
            return
        for task in self._tasks.values():
            if (
                task.kind == REMINDER
                and task.status == OPEN
                and task.period_id == batch.period_id
                and task.source == batch.source
                and task.mode is not None
                and self._expectation_satisfied(task.period_id, task.source, task.mode)
            ):
                task.status = DONE

    def tick(self, today: date) -> list[Task]:
        """返回截止日已到且仍未完成的任务（提醒与待发布）。"""
        return sorted(
            (t for t in self._tasks.values() if t.status == OPEN and t.due <= today),
            key=lambda t: (t.due, t.task_id),
        )

    def recover(self) -> dict[str, list[Task]]:
        """服务恢复后继续跟踪的未完成提醒与待发布任务。"""
        open_tasks = sorted(
            (t for t in self._tasks.values() if t.status == OPEN),
            key=lambda t: (t.due, t.task_id),
        )
        return {
            "reminders": [t for t in open_tasks if t.kind == REMINDER],
            "pending_publish": [t for t in open_tasks if t.kind == PUBLISH_TASK],
        }
