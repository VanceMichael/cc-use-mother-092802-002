"""追加式事件日志存储：服务恢复时回放事件重建状态。

存储布局：
  store_dir/events.logl   追加写入的领域事件（每行一个 JSON，含单调 seq）
  store_dir/snapshot.json 原子快照（tmp + fsync + rename），加速重启

重启恢复时：先读快照，再只回放快照之后的事件；事件永不原地修改，
因此任何历史发布版本与审计轨迹都可以重建。
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Callable, Iterable


class EventStore:
    """单行 JSON 事件日志，附带可随时刷新的原子快照。"""

    def __init__(self, store_dir: str | Path):
        self.dir = Path(store_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.events_path = self.dir / "events.logl"
        self.snapshot_path = self.dir / "snapshot.json"
        self._truncate_torn_tail()

    def _truncate_torn_tail(self) -> None:
        """崩溃可能在末尾留下未写完的半行；重启时截掉它，使后续追加合法。

        中间行损坏不处理（iter_events 会显式报错），只允许修复真正的文件尾。
        """
        if not self.events_path.exists():
            return
        raw = self.events_path.read_bytes()
        if not raw:
            return
        lines = raw.splitlines(keepends=True)
        valid_end = 0
        offset = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                offset += len(line)
                continue
            try:
                json.loads(stripped)
            except (ValueError, json.JSONDecodeError):
                if i != len(lines) - 1:
                    return  # 中间损坏：不静默处理
                # 末尾半行：截到最后一条完整事件之后
                os.truncate(self.events_path, valid_end)
                return
            valid_end = offset + len(line)
            offset += len(line)

    # ---- 写入 ----
    def append(self, event_type: str, payload: dict[str, Any], ts: str) -> dict[str, Any]:
        event = {"seq": self.last_seq() + 1, "type": event_type, "ts": ts, "payload": payload}
        with self.events_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        return event

    def last_seq(self) -> int:
        """直接读取主日志最后一行的 seq；容忍崩溃留下的末尾半行。"""
        if not self.events_path.exists() or self.events_path.stat().st_size == 0:
            return 0
        with self.events_path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            end = fh.tell()
            block = 1024
            data = b""
            pos = end
            while pos > 0:
                step = min(block, pos)
                pos -= step
                fh.seek(pos)
                data = fh.read(step) + data
                lines = data.splitlines()
                if len(lines) > 1 or pos == 0:
                    for line in reversed(lines):
                        try:
                            return int(json.loads(line)["seq"])
                        except (ValueError, json.JSONDecodeError):
                            continue
        return 0

    # ---- 读取 ----
    def iter_events(self, after_seq: int = 0) -> Iterable[dict[str, Any]]:
        if not self.events_path.exists():
            return
        with self.events_path.open(encoding="utf-8") as fh:
            lines = fh.readlines()
        for index, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                # 仅允许丢弃文件末尾因崩溃产生的半行，中间损坏必须暴露
                if any(l.strip() for l in lines[index + 1:]):
                    raise
                continue
            if event["seq"] > after_seq:
                yield event

    def save_snapshot(self, state_jsonable: dict[str, Any]) -> None:
        snap = {"last_seq": state_jsonable["_last_seq"], "state": state_jsonable}
        fd, tmp = tempfile.mkstemp(dir=self.dir, prefix=".snap-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(snap, fh, ensure_ascii=False)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.snapshot_path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def load_snapshot(self) -> dict[str, Any] | None:
        if not self.snapshot_path.exists():
            return None
        return json.loads(self.snapshot_path.read_text(encoding="utf-8"))


_clock: Callable[[], str] | None = None


def set_clock(fn: Callable[[], str] | None) -> None:
    """注入时钟（测试用）；传入 None 恢复系统时钟。"""
    global _clock
    _clock = fn


def now() -> str:
    if _clock is not None:
        return _clock()
    from datetime import datetime

    return datetime.now().isoformat()
