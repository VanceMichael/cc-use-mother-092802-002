"""归集服务的持久化：内存实现与 JSON 文件实现。

服务把每个集合（批次、隔离区、调整、版本、任务等）作为一个 JSON 值
整体读写；文件实现采用临时文件加替换的方式，避免写入中断留下坏文件。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Protocol


class Store(Protocol):
    def load(self, collection: str) -> Any | None:
        """读取集合内容，不存在时返回 None。"""
        ...

    def save(self, collection: str, value: Any) -> None:
        """整体覆盖写入集合内容。"""
        ...


class MemoryStore:
    """进程内存储，用于测试或一次性计算。"""

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    def load(self, collection: str) -> Any | None:
        return self._data.get(collection)

    def save(self, collection: str, value: Any) -> None:
        self._data[collection] = value


class JsonFileStore:
    """按集合一个 JSON 文件的目录存储，服务重启后由此恢复。"""

    def __init__(self, directory: Path | str) -> None:
        self._dir = Path(directory)
        self._dir.mkdir(parents=True, exist_ok=True)

    def _path(self, collection: str) -> Path:
        return self._dir / f"{collection}.json"

    def load(self, collection: str) -> Any | None:
        path = self._path(collection)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def save(self, collection: str, value: Any) -> None:
        path = self._path(collection)
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        os.replace(temporary, path)
