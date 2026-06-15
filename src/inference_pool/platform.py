"""统一推理算力池 — 顶层编排门面（把三层 + autoscaler + 亲和路由串起来）。

控制回路（每个周期）：
    1. gateway.metrics() 采集在线负载
    2. autoscaler.compute_target() 决定目标在线卡数（含预热/迟滞）
    3. 扩容前：若需 reload 离线卡，先 drain 其在途分片（保证续跑）
    4. card_pool.scale_online() 执行温卡优先回收 + 必要时换装
    5. gateway.set_online_cards() 用"立即可服务"的在线卡数（温卡秒级、换装需 tick）
    6. 高峰压低批量填谷
唯一硬不变量（§3.1）：在线副本数始终够扛当前 QPS。
"""
from __future__ import annotations

from .autoscaler import Autoscaler, AutoscalerConfig
from .card_pool import CardPool, ScalePlan
from .cache_affinity import CacheAffinityRouter
from .gateway import InferenceGateway
from .models import Card, CardRole, ModelSpec
from .offline_pool import OfflinePool


class Platform:
    def __init__(
        self,
        pool: CardPool,
        gateway: InferenceGateway,
        autoscaler: Autoscaler,
        offline: OfflinePool,
    ):
        self.pool = pool
        self.gateway = gateway
        self.autoscaler = autoscaler
        self.offline = offline

    def control_step(self, now: float | None = None) -> ScalePlan:
        """跑一个控制周期，返回本次的扩缩计划（供观测/断言）。"""
        # 先完成已就绪的换装/drain。
        self.pool.tick(now)

        m = self.gateway.metrics()
        target = self.autoscaler.compute_target(m, now)

        before = self.pool.online_cards()
        if target > before:
            # 扩容：温卡不足而需 reload 离线卡时，先 drain 候选离线卡的在途分片（续跑保证）。
            warm = self.pool.warm_cards()
            need_reload = max(0, (target - before) - warm)
            if need_reload > 0:
                candidates = [
                    c.card_id for c in self.pool.cards_by_role(CardRole.OFFLINE)[:need_reload]
                ]
                self.offline.reclaim_for_online(candidates)

        plan = self.pool.scale_online(target)
        # gateway 立即可用的在线卡 = 温卡回收已秒级到位；换装中的卡要等 tick 才上线。
        self.gateway.set_online_cards(plan.instant_online)
        # 仅当在线需求超过当前 slot（真正高峰、会饿着在线）才压低批量填谷；
        # 平稳期保持批量填谷以吃掉空泡（§5.2 不搬卡的核心日常）。
        self.gateway.pause_batch(m.queue_depth > self.gateway.online_slots)
        return plan
