"""对象存储抽象（存储面）。

本地文件系统实现，对应设计里的"统一对象存储"。关键语义：
- 原子写：先写临时文件再 rename（§6.3 幂等提交前提）
- range read：worker 只读自己那段分片字节（§6.2 切指针不切数据）
"""
from __future__ import annotations

import os
from pathlib import Path


class LocalObjectStore:
    def __init__(self, root: str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        # key 不允许逃逸根目录
        p = (self.root / key.lstrip("/")).resolve()
        if not str(p).startswith(str(self.root.resolve())):
            raise ValueError(f"key escapes store root: {key}")
        return p

    def write(self, key: str, data: bytes) -> None:
        """原子写：tmp → rename。最终路径要么完整要么不存在。"""
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)  # 原子

    def read(self, key: str) -> bytes:
        with open(self._path(key), "rb") as f:
            return f.read()

    def read_range(self, key: str, start: int, end: int) -> bytes:
        """读 [start, end) 字节区间 —— worker 用它只取自己那段分片。"""
        with open(self._path(key), "rb") as f:
            f.seek(start)
            return f.read(end - start)

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def size(self, key: str) -> int:
        return self._path(key).stat().st_size

    def list(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not base.exists():
            return []
        root = self.root.resolve()
        out = []
        for p in base.rglob("*"):
            if p.is_file() and not p.name.endswith(".tmp"):
                out.append(str(p.resolve().relative_to(root)))
        return sorted(out)
