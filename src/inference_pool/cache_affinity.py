"""prefix / KV cache 亲和路由（§6.4，Claude Code 专项）。

Claude Code 是 agentic 负载：单任务几十上百次调用、长 context、反复读同一 repo。
KV / prefix cache 命中率直接决定吞吐与延迟。原文把它划到本期之外，本场景下
作为一等需求纳入（R4.3）。

策略：把同一 session/repo 前缀的请求**粘性路由**到持有相应 prefix cache 的副本。
副本扩缩时，仅被移除副本上的会话需要重新落位（miss），其余保持命中。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .models import Request


class CacheAffinityRouter:
    def __init__(self, capacity: int = 0):
        self._capacity = capacity
        self._pin: dict[str, int] = {}  # session_key -> replica index
        self.hits = 0
        self.misses = 0

    def set_capacity(self, n: int) -> None:
        """在线副本数变化。被移除副本上的会话失去亲和（下次路由为 miss）。"""
        self._capacity = max(0, n)
        if self._capacity == 0:
            self._pin.clear()
            return
        # 落在已不存在副本上的 pin 失效。
        for key, idx in list(self._pin.items()):
            if idx >= self._capacity:
                del self._pin[key]

    def route(self, req: Request) -> bool:
        """路由一个在线请求；返回是否命中 prefix cache。"""
        if self._capacity == 0:
            return False
        key = req.session_key
        if key is None:
            # 无会话标识：按请求 id 散列落位，不计亲和命中。
            return False
        idx = self._pin.get(key)
        if idx is not None and idx < self._capacity:
            self.hits += 1
            return True
        # 冷会话或副本已变更：落到稳定散列副本，建立 pin（本次 miss，后续命中）。
        self._pin[key] = (hash(key) & 0x7FFFFFFF) % self._capacity
        self.misses += 1
        return False

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total else 0.0
