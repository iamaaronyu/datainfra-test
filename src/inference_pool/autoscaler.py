"""在线副本自动扩缩（§6.2，需求文档明确的"真正关键路径"）。

47 倍峰谷比（3000:64）下，决定 vibecoding 体验的不是"抢没抢到卡"，而是
"副本能不能在 QPS 涨上来之前就位"。因此本模块做三件事：

  R2.1  按 TTFT / 队列深度 / QPS 计算目标在线卡数（64–3000）。
  R2.3  预测性预热：按历史曲线在早高峰前抬高目标下限，避免冷启动落在堆积窗口。
  R2.4  缩容迟滞 / 冷却：扩容立即响应，缩容需"持续低于"一段时间才执行，防抖。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field

from .models import LoadMetrics


@dataclass
class AutoscalerConfig:
    min_cards: int = 64           # 深夜兜底下限（§4.1）
    max_cards: int = 3000         # 早高峰上限（§4.1）
    qps_per_card: float = 2.0     # 单卡可承载的稳态 QPS（引擎基线）
    headroom: float = 0.2         # 余量系数：留 20% 防尖峰
    queue_depth_per_card: float = 4.0   # 每卡可消化的排队深度
    ttft_target: float = 1.0      # TTFT P95 目标（秒）；超标按比例追加副本
    scale_down_cooldown: float = 300.0  # 缩容冷却（秒）：持续低于该时长才缩
    scale_step_down: float = 0.5  # 单次缩容最多回收当前与目标差的比例（迟滞）


@dataclass
class PrewarmWindow:
    """预测性预热窗口（§5.1 / R2.3）：[start,end) 期间在线下限抬到 floor_cards。"""

    start: float
    end: float
    floor_cards: int


class Autoscaler:
    def __init__(self, config: AutoscalerConfig | None = None, clock=time.time):
        self.cfg = config or AutoscalerConfig()
        self.clock = clock
        self.prewarm: list[PrewarmWindow] = []
        self._low_since: float | None = None  # 进入"可缩容"区间的起点（迟滞用）
        self._current_target = self.cfg.min_cards

    def add_prewarm(self, window: PrewarmWindow) -> None:
        self.prewarm.append(window)

    def _demand_cards(self, m: LoadMetrics) -> int:
        """纯需求侧目标：综合 QPS / 队列深度 / TTFT，取最大者。"""
        cfg = self.cfg
        by_qps = m.qps / cfg.qps_per_card * (1 + cfg.headroom)
        by_queue = m.queue_depth / cfg.queue_depth_per_card
        demand = max(by_qps, by_queue)
        # TTFT 超标：说明现有副本不够，按超标比例追加。
        if m.ttft_p95 > cfg.ttft_target and demand > 0:
            demand *= m.ttft_p95 / cfg.ttft_target
        return int(demand + 0.999)  # ceil

    def _prewarm_floor(self, now: float) -> int:
        floor = self.cfg.min_cards
        for w in self.prewarm:
            if w.start <= now < w.end:
                floor = max(floor, w.floor_cards)
        return floor

    def compute_target(self, m: LoadMetrics, now: float | None = None) -> int:
        """返回本周期目标在线卡数（已夹在 [min,max]、已含预热下限与缩容迟滞）。"""
        now = self.clock() if now is None else now
        cfg = self.cfg

        raw = max(self._demand_cards(m), self._prewarm_floor(now))
        raw = max(cfg.min_cards, min(cfg.max_cards, raw))

        prev = self._current_target
        if raw >= prev:
            # 扩容（或持平）：立即响应，清除缩容计时。
            self._low_since = None
            target = raw
        else:
            # 缩容：需"持续低于"冷却时长，且单步只回收一半差额（迟滞防抖，§5.4）。
            if self._low_since is None:
                self._low_since = now
            if now - self._low_since >= cfg.scale_down_cooldown:
                step = int((prev - raw) * cfg.scale_step_down + 0.999)
                target = max(raw, prev - max(step, 1))
                # 已缩一步，重置计时，下一周期持续低再继续缩。
                self._low_since = now
            else:
                target = prev  # 冷却未到，维持，不抖动

        target = max(cfg.min_cards, min(cfg.max_cards, target))
        self._current_target = target
        return target
