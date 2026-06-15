"""卡池状态机（§2 三层架构 / §3 核心不变量 / §4 资源模型）。

这是把"搬卡式抢占"换成"角色转换"的核心。一张卡在不同时刻扮演不同角色：

    OFFLINE  ──reload(分钟级)──▶  LOADING ──ready──▶ ONLINE / OFFLINE(新模型)
    ONLINE   ──release(秒级)───▶  WARM_BATCH      （保留 GLM5.1 权重，跑批量填谷）
    WARM_BATCH ──reclaim(秒级)─▶  ONLINE          （温卡优先，R2.2）
    OFFLINE  ──drain──────────▶  DRAINING ──done──▶ 可换装

关键不变量（§3）：
  1. 在线 SLA 优先：reconcile 永远先满足在线目标卡数。
  2. 混跑不搬卡：在线/离线 GLM5.1 在 gateway 靠优先级共存，这里不"抢"。
  3. 温卡优先：扩容先回收 WARM_BATCH（免换装、秒级），不足再 reload OFFLINE。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field

from .models import Card, CardRole, CardType, ModelSpec, ONLINE_MODEL


@dataclass
class _Transition:
    target_role: CardRole
    target_model: str | None
    ready_at: float


@dataclass
class ScalePlan:
    """一次在线扩缩的结果（供观测与断言，§5.1 递进式扩容）。"""

    target: int
    before: int
    warm_reclaimed: list[str] = field(default_factory=list)   # 温卡秒级转在线
    reload_started: list[str] = field(default_factory=list)   # 离线卡换装为在线（分钟级）
    released: list[str] = field(default_factory=list)         # 缩容：在线→温卡
    shortfall: int = 0                                        # 卡池不足以达成目标的缺口

    @property
    def instant_online(self) -> int:
        """立即可服务在线的卡数（温卡回收是秒级）。"""
        return self.before + len(self.warm_reclaimed) - len(self.released)


class CardPool:
    """统一卡池。管理 ~1.5 万张卡的角色与换装时序。"""

    def __init__(
        self,
        cards: list[Card],
        specs: dict[str, ModelSpec],
        clock=time.time,
    ):
        self._cards: dict[str, Card] = {c.card_id: c for c in cards}
        self._specs = specs
        self._pending: dict[str, _Transition] = {}
        self.clock = clock

    # ---- 查询 ------------------------------------------------------------
    @property
    def total(self) -> int:
        return len(self._cards)

    def cards_by_role(self, role: CardRole) -> list[Card]:
        return [c for c in self._cards.values() if c.role == role]

    def counts(self) -> dict[CardRole, int]:
        out = {r: 0 for r in CardRole}
        for c in self._cards.values():
            out[c.role] += 1
        return out

    def online_cards(self) -> int:
        return sum(1 for c in self._cards.values() if c.role == CardRole.ONLINE)

    def warm_cards(self) -> int:
        return sum(1 for c in self._cards.values() if c.role == CardRole.WARM_BATCH)

    def utilization(self) -> float:
        """整池利用率：ONLINE/WARM_BATCH/OFFLINE 均算"在干活"，LOADING/DRAINING 不算（§7.3）。"""
        if not self._cards:
            return 0.0
        busy = sum(
            1
            for c in self._cards.values()
            if c.role in (CardRole.ONLINE, CardRole.WARM_BATCH, CardRole.OFFLINE)
        )
        return busy / len(self._cards)

    # ---- 时序推进 --------------------------------------------------------
    def tick(self, now: float | None = None) -> list[Card]:
        """完成所有已就绪的换装 / drain。返回本次完成转换的卡。"""
        now = self.clock() if now is None else now
        done: list[Card] = []
        for cid, tr in list(self._pending.items()):
            if now >= tr.ready_at:
                card = self._cards[cid]
                card.role = tr.target_role
                card.model = tr.target_model
                del self._pending[cid]
                done.append(card)
        return done

    # ---- 在线扩缩（关键路径，§6.2）--------------------------------------
    def scale_online(self, target: int) -> ScalePlan:
        """把在线副本卡数 reconcile 到 target。

        扩容递进（§5.1）：先回收温卡（秒级）→ 仍不足再 reload 离线卡（分钟级）。
        缩容（§5.4）：多余在线卡退为温卡（保留 GLM5.1 权重，秒级，作温卡缓冲）。
        """
        now = self.clock()
        before = self.online_cards()
        plan = ScalePlan(target=target, before=before)

        if target > before:
            need = target - before
            # 1) 温卡优先：WARM_BATCH 已持有 GLM5.1 权重，秒级转在线。
            for card in self.cards_by_role(CardRole.WARM_BATCH):
                if need == 0:
                    break
                self._pending.pop(card.card_id, None)
                card.role = CardRole.ONLINE
                card.model = ONLINE_MODEL
                plan.warm_reclaimed.append(card.card_id)
                need -= 1
            # 2) 仍不足：从离线池回收卡换装为 GLM5.1 在线副本（分钟级）。
            if need > 0:
                load_s = self._specs[ONLINE_MODEL].load_seconds
                for card in self.cards_by_role(CardRole.OFFLINE):
                    if need == 0:
                        break
                    card.role = CardRole.LOADING
                    self._pending[card.card_id] = _Transition(
                        CardRole.ONLINE, ONLINE_MODEL, now + load_s
                    )
                    plan.reload_started.append(card.card_id)
                    need -= 1
            plan.shortfall = need

        elif target < before:
            excess = before - target
            for card in self.cards_by_role(CardRole.ONLINE):
                if excess == 0:
                    break
                card.role = CardRole.WARM_BATCH  # 保留 GLM5.1 权重，跑批量填谷
                plan.released.append(card.card_id)
                excess -= 1

        return plan

    # ---- 离线换装（§6.3 R3.1/R3.2）-------------------------------------
    def reload_to(
        self, card_id: str, model: str, role: CardRole = CardRole.OFFLINE
    ) -> None:
        """把某张卡换装为指定模型（卸载旧 + 加载新，分钟级）。"""
        card = self._cards[card_id]
        now = self.clock()
        load_s = self._specs[model].load_seconds
        card.role = CardRole.LOADING
        self._pending[card_id] = _Transition(role, model, now + load_s)

    def drain(self, card_id: str, drain_seconds: float) -> None:
        """对某张离线卡 drain：停止派发新分片，在途分片跑完后可换装（R3.2）。"""
        card = self._cards[card_id]
        now = self.clock()
        card.role = CardRole.DRAINING
        self._pending[card_id] = _Transition(CardRole.OFFLINE, card.model, now + drain_seconds)
