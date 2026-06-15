"""多租户配额（§8）。

入队时校验：超额排队/降级。这里实现卡数硬边界 reserve/release，
对应 Volcano 队列 capability 的兜底语义。

.. deprecated:: NPU 统一推理算力池 v2
    新需求不再用"业务百分比/租户卡配额"做在线-离线分配（v2 §10）；在线由 autoscaler
    按负载定副本数，离线吞吐由空泡填谷 + 常驻池自然兜底。仅旧 compute_platform 仍用本模块。
"""
from __future__ import annotations

import threading


class QuotaExceeded(Exception):
    pass


class QuotaManager:
    def __init__(self, quotas: dict[str, int]):
        self._quotas = dict(quotas)        # tenant -> 卡数上限
        self._used: dict[str, int] = {}
        self._lock = threading.Lock()

    def limit(self, tenant: str) -> int:
        return self._quotas.get(tenant, 0)

    def used(self, tenant: str) -> int:
        with self._lock:
            return self._used.get(tenant, 0)

    def available(self, tenant: str) -> int:
        with self._lock:
            return self._quotas.get(tenant, 0) - self._used.get(tenant, 0)

    def reserve(self, tenant: str, cards: int) -> None:
        with self._lock:
            limit = self._quotas.get(tenant, 0)
            used = self._used.get(tenant, 0)
            if used + cards > limit:
                raise QuotaExceeded(
                    f"tenant={tenant} quota {limit}, used {used}, requested {cards}")
            self._used[tenant] = used + cards

    def try_reserve(self, tenant: str, cards: int) -> bool:
        try:
            self.reserve(tenant, cards)
            return True
        except QuotaExceeded:
            return False

    def release(self, tenant: str, cards: int) -> None:
        with self._lock:
            self._used[tenant] = max(0, self._used.get(tenant, 0) - cards)
