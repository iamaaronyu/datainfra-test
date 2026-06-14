"""弹性 worker 池控制器（§6.4）。

纯函数，便于全量测试。核心公式：
    desired = min(按积压, 按空闲卡, 按配额)
再过"加载成本门槛"：只有新 worker 预期工作量 ≥ K×加载时间 才值得扩，
防止为一点尾巴活反复起停大实例。
潮汐末班车：预测抢占窗口前 N×加载时间不再补充大 worker。
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from ..models import ModelSpec


@dataclass
class PoolDemand:
    backlog_rows: int        # 队列积压条数（待领取分片总行数）
    free_cards: int          # 空闲卡数（已减安全水位）
    quota_cards: int         # 该租户剩余配额卡数
    target_runtime_seconds: int = 600  # 期望多久把积压清空


def desired_workers(demand: PoolDemand, spec: ModelSpec, k: int) -> int:
    """计算该模型池目标 worker 数。"""
    if demand.backlog_rows <= 0:
        return 0

    # 总工作量（单 worker 串行秒数）
    work_seconds = demand.backlog_rows / spec.throughput_rows_per_sec

    # 1) 按积压：要在 target_runtime 内清空需要几个 worker
    by_backlog = math.ceil(work_seconds / demand.target_runtime_seconds)
    # 2) 按空闲卡供给
    by_supply = demand.free_cards // spec.cards_per_worker
    # 3) 按配额
    by_quota = demand.quota_cards // spec.cards_per_worker

    desired = min(by_backlog, by_supply, by_quota)

    # 4) 加载成本门槛：每个 worker 摊到的工作量须 ≥ K×加载时间
    if desired > 0:
        max_by_loadcost = int(work_seconds // (k * spec.load_seconds))
        desired = min(desired, max(max_by_loadcost, 0))
        # 至少允许 1 个（只要有活且供给/配额允许），避免门槛把小积压卡死在 0：
        # 仅当单 worker 自身工作量也达不到门槛才真正归零
        if desired == 0 and work_seconds >= k * spec.load_seconds \
                and by_supply >= 1 and by_quota >= 1:
            desired = 1
    return max(desired, 0)


def should_add_big_worker(
    now: float,
    predicted_preempt_at: float | None,
    spec: ModelSpec,
    n: int,
) -> bool:
    """末班车判定：现在起一个大 worker，能否在被抢占前至少干够 N×加载时间的活。

    predicted_preempt_at 为 None 表示无预测抢占（如深夜），直接放行。
    """
    if predicted_preempt_at is None:
        return True
    return now + n * spec.load_seconds < predicted_preempt_at
