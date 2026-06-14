"""计量结算（§8）—— 价格杠杆。

规则：
- 按卡·时计价，base 单价 × 卡型倍率（B3 > B4，异构差异定价）
- Guaranteed 全价；Preemptible 折价（如 4 折）
- Preemptible 被抢占的那段时间不计费（否则没人愿标可抢占）

调度核心每次绑卡/释放上报 UsageEvent，本服务累计成本。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass

from ..config import Config
from ..models import CardType, QoS


@dataclass
class UsageEvent:
    tenant: str
    card_type: CardType
    qos: QoS
    cards: int
    seconds: float
    preempted_seconds: float = 0.0  # 仅 Preemptible 有意义；这段不计费


class MeteringService:
    def __init__(self, config: Config):
        self.config = config
        self._cost: dict[str, float] = {}
        self._lock = threading.Lock()

    def _card_factor(self, card_type: CardType) -> float:
        return (self.config.b3_price_factor if card_type == CardType.B3
                else self.config.b4_price_factor)

    def _qos_factor(self, qos: QoS) -> float:
        if qos == QoS.PREEMPTIBLE:
            return self.config.preemptible_discount
        return 1.0  # Guaranteed / BestEffort-Fixed 全价

    def cost_of(self, ev: UsageEvent) -> float:
        billable_seconds = ev.seconds
        if ev.qos == QoS.PREEMPTIBLE:
            billable_seconds = max(0.0, ev.seconds - ev.preempted_seconds)
        hours = billable_seconds / 3600.0
        return (hours * ev.cards
                * self.config.base_price_per_card_hour
                * self._card_factor(ev.card_type)
                * self._qos_factor(ev.qos))

    def record(self, ev: UsageEvent) -> float:
        cost = self.cost_of(ev)
        with self._lock:
            self._cost[ev.tenant] = self._cost.get(ev.tenant, 0.0) + cost
        return cost

    def bill(self) -> dict[str, float]:
        with self._lock:
            return dict(self._cost)
