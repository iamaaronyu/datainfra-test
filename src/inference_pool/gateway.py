"""推理混跑层（§2 第一层 / §6.1）。

吃掉了原需求 ~80% 复杂度的核心：GLM5.1 **单部署**同时承载在线（Claude Code）
与离线批量两类请求，靠推理引擎的"连续批处理 + 请求级优先级"实现：

  - 在线请求高优先级，每个调度步先被排满（实时插队，R1.2）。
  - 批量请求低优先级，只填在线留下的空泡（填谷，R1.4），高峰可被压低/暂停。
  - 二者跑在**同一批副本**里，不发生任何物理搬卡（§5.2 核心日常）。

"在线 SLA 不被批量拖累"在此被建模为：每一步都先满足在线，批量只拿剩余 slot；
因此只要在线 slot ≥ 在线到达量，批量再多也不会推迟在线。
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from .models import LoadMetrics, Priority, Request


@dataclass
class StepResult:
    served_online: list[str] = field(default_factory=list)
    served_batch: list[str] = field(default_factory=list)
    online_waiting: int = 0   # 本步未被服务的在线请求（slot 不够 → 触发扩容信号）
    cache_hits: int = 0       # 命中 prefix/KV cache 的在线请求数（§6.4）


class InferenceGateway:
    """GLM5.1 单部署的请求混跑网关。"""

    def __init__(self, slots_per_card: int = 4, router=None):
        self.slots_per_card = slots_per_card
        self.router = router  # 可选 CacheAffinityRouter（§6.4）
        self.online_q: deque[Request] = deque()
        self.batch_q: deque[Request] = deque()
        self._online_cards = 0
        self.batch_paused = False
        # 累计指标（§6.6 R6.1）
        self.total_online = 0
        self.total_batch = 0
        self.total_online_waited = 0  # 累计被推迟的在线请求（SLA 违约计数）
        self.total_cache_hits = 0
        self._last_offered = 0  # 上一步在线被提供的请求量（服务+排队），作瞬时 QPS 代理

    # ---- 容量与开关 ------------------------------------------------------
    def set_online_cards(self, n: int) -> None:
        """autoscaler / card_pool 决定的当前在线副本卡数。"""
        self._online_cards = max(0, n)
        if self.router is not None:
            self.router.set_capacity(self._online_cards)

    def pause_batch(self, paused: bool = True) -> None:
        """高峰压低/暂停批量填谷（R1.4）。"""
        self.batch_paused = paused

    @property
    def online_slots(self) -> int:
        return self._online_cards * self.slots_per_card

    # ---- 提交 ------------------------------------------------------------
    def submit(self, req: Request) -> None:
        if req.priority == Priority.ONLINE:
            self.online_q.append(req)
        else:
            self.batch_q.append(req)

    # ---- 调度步（连续批处理）-------------------------------------------
    def step(self) -> StepResult:
        """一个连续批处理步：先排满在线，剩余 slot 给批量填谷。"""
        res = StepResult()
        cap = self.online_slots

        # 1) 在线优先：实时插队，最多 cap 个。
        n_online = min(len(self.online_q), cap)
        for _ in range(n_online):
            req = self.online_q.popleft()
            hit = False
            if self.router is not None:
                hit = self.router.route(req)
            if hit:
                res.cache_hits += 1
                self.total_cache_hits += 1
            res.served_online.append(req.request_id)
        self.total_online += n_online

        # 在线未被服务的部分 = SLA 压力信号（slot 不够）。
        res.online_waiting = len(self.online_q)
        self.total_online_waited += res.online_waiting
        # 本步在线总需求（已服务 + 仍排队）= 瞬时 QPS 代理，喂给 autoscaler。
        self._last_offered = n_online + res.online_waiting

        # 2) 批量填谷：只用在线剩下的 slot，且可被暂停（§5.2 不搬卡）。
        remaining = cap - n_online
        if not self.batch_paused and remaining > 0:
            n_batch = min(len(self.batch_q), remaining)
            for _ in range(n_batch):
                req = self.batch_q.popleft()
                res.served_batch.append(req.request_id)
            self.total_batch += n_batch

        return res

    # ---- 指标 ------------------------------------------------------------
    def metrics(self) -> LoadMetrics:
        """给 autoscaler 的实时负载快照（§6.2 R2.1）。"""
        depth = len(self.online_q)
        # 简化的 TTFT 模型：在线排队越深于容量，TTFT 越高。
        cap = max(self.online_slots, 1)
        ttft = 0.2 + depth / cap  # 基线 0.2s + 排队惩罚
        # qps 用上一步在线总需求（瞬时代理），含当前仍排队部分。
        qps = float(max(self._last_offered, depth))
        return LoadMetrics(qps=qps, queue_depth=depth, ttft_p95=ttft)

    def utilization(self) -> float:
        """在线副本利用率：本步实际占用的 slot 比例（§7.1 空泡被填）。"""
        cap = self.online_slots
        if cap == 0:
            return 0.0
        used = min(len(self.online_q), cap)
        if not self.batch_paused:
            used = cap if (len(self.batch_q) + len(self.online_q)) >= cap else len(self.online_q) + len(self.batch_q)
        return min(1.0, used / cap)
