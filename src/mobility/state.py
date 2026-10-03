"""事件归并：回放事件日志重建内存状态；快照的 JSON 编解码。"""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from datetime import datetime
from typing import Any

from .models import (
    Adjustment,
    BatchRecord,
    Caliber,
    PublishTask,
    Source,
)


def initial_state() -> dict[str, Any]:
    return {
        "_last_seq": 0,
        "holiday": None,        # {"start", "end", "days", "name"}
        "sources": {},          # source_id -> Source
        "calibers": {},         # caliber_id -> Caliber
        "batches": {},          # 内部 batch_id -> BatchRecord
        "display_index": {},    # 报送编号 -> 内部 batch_id
        "quarantine": {},       # 隔离内部 id -> BatchRecord
        "adjustments": {},      # adjustment_id -> Adjustment
        "tasks": {},            # task_id -> PublishTask
        "reminders": {},        # 去重键 -> 最近提醒 ISO 时间
        "derived": {},          # 指标键 -> {"fingerprint", "result"}
        "publications": [],     # 发布版本快照（dict），按版本递增
    }


def apply_event(state: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    p = event.get("payload", {})
    t = event["type"]
    state["_last_seq"] = event["seq"]

    if t == "holiday_opened":
        state["holiday"] = p
    elif t == "source_registered":
        s = Source(**p)
        state["sources"][s.source_id] = s
    elif t == "caliber_defined":
        c = Caliber.from_dict(p)
        state["calibers"][c.caliber_id] = c
    elif t in ("batch_received", "batch_duplicated", "batch_quarantined",
               "batch_revision_received", "batch_rejected"):
        b = BatchRecord.from_dict(p["batch"])
        state["batches"][b.batch_id] = b
        if b.status == "accepted" and b.display_id is not None:
            state["display_index"][f"{b.source_id}/{b.display_id}"] = b.batch_id
        elif b.status in ("rejected", "duplicate") and b.display_id is not None:
            # 被退回/判重的编号不再占用，来源可用原编号重新报送
            key = f"{b.source_id}/{b.display_id}"
            if state["display_index"].get(key) == b.batch_id:
                state["display_index"].pop(key, None)
        if t == "batch_quarantined":
            state["quarantine"][b.batch_id] = b
    elif t == "batch_superseded":
        new_b = BatchRecord.from_dict(p["batch"])
        state["batches"][new_b.batch_id] = new_b
        if new_b.display_id is not None:
            state["display_index"][f"{new_b.source_id}/{new_b.display_id}"] = new_b.batch_id
        old = state["batches"].get(p["superseded_id"])
        if old is not None:
            old.status = "superseded"
            old.superseded_by = new_b.batch_id
    elif t == "quarantine_resolved":
        qid = p["quarantined_id"]
        state["quarantine"].pop(qid, None)
        target = state["batches"].get(qid)
        if p["decision"] == "accept":
            replacement = BatchRecord.from_dict(p["replacement"])
            state["batches"][replacement.batch_id] = replacement
            if replacement.display_id is not None:
                state["display_index"][f"{replacement.source_id}/{replacement.display_id}"] = replacement.batch_id
            old = state["batches"].get(p["conflicting_with"])
            if old is not None:
                old.status = "superseded"
                old.superseded_by = replacement.batch_id
            if target is not None:
                target.status = "rejected"
                target.resolution = "replaced_by_accepted_resolution"
        elif target is not None:
            target.status = "rejected"
            target.resolution = p.get("note", "discarded")
    elif t == "adjustment_proposed":
        a = Adjustment.from_dict(p)
        state["adjustments"][a.adjustment_id] = a
    elif t == "adjustment_reviewed":
        a = state["adjustments"][p["adjustment_id"]]
        a.status = p["status"]
        a.reviewed_by = p["reviewed_by"]
        a.reviewed_at = datetime.fromisoformat(p["reviewed_at"]) if p.get("reviewed_at") else None
        a.review_note = p.get("review_note")
    elif t == "derived_computed":
        state["derived"][p["key"]] = {"fingerprint": p["fingerprint"], "result": p["result"]}
    elif t in ("published", "corrigendum_issued"):
        state["publications"].append(p["version"])
    elif t == "task_created":
        task = PublishTask.from_dict(p)
        state["tasks"][task.task_id] = task
    elif t in ("task_completed", "task_cancelled"):
        task = state["tasks"][p["task_id"]]
        task.status = "completed" if t == "task_completed" else "cancelled"
        task.completed_at = datetime.fromisoformat(p["completed_at"]) if p.get("completed_at") else None
    elif t == "reminder_fired":
        state["reminders"][p["key"]] = event["ts"]
    return state


# ---------------- 快照编解码 ----------------
def to_jsonable(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "_last_seq": state["_last_seq"],
        "holiday": state["holiday"],
        "sources": {k: v.__dict__ for k, v in state["sources"].items()},
        "calibers": {k: v.to_dict() for k, v in state["calibers"].items()},
        "batches": {k: v.to_dict() for k, v in state["batches"].items()},
        "display_index": dict(state["display_index"]),
        "quarantine": list(state["quarantine"].keys()),
        "adjustments": {k: v.to_dict() for k, v in state["adjustments"].items()},
        "tasks": {k: v.to_dict() for k, v in state["tasks"].items()},
        "reminders": dict(state["reminders"]),
        "derived": _jsonable(state["derived"]),
        "publications": _jsonable(state["publications"]),
    }


def from_jsonable(raw: dict[str, Any]) -> dict[str, Any]:
    state = initial_state()
    state["_last_seq"] = raw["_last_seq"]
    state["holiday"] = raw.get("holiday")
    state["sources"] = {k: Source(**v) for k, v in raw.get("sources", {}).items()}
    state["calibers"] = {k: Caliber.from_dict(v) for k, v in raw.get("calibers", {}).items()}
    state["batches"] = {k: BatchRecord.from_dict(v) for k, v in raw.get("batches", {}).items()}
    state["display_index"] = dict(raw.get("display_index", {}))
    state["quarantine"] = {k: state["batches"][k] for k in raw.get("quarantine", []) if k in state["batches"]}
    state["adjustments"] = {k: Adjustment.from_dict(v) for k, v in raw.get("adjustments", {}).items()}
    state["tasks"] = {k: PublishTask.from_dict(v) for k, v in raw.get("tasks", {}).items()}
    state["reminders"] = dict(raw.get("reminders", {}))
    state["derived"] = raw.get("derived", {})
    state["publications"] = raw.get("publications", [])
    return state


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return {f.name: _jsonable(getattr(value, f.name)) for f in fields(value)}
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value
