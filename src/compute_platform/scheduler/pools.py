"""三池模型 —— 决策三的内核（架构 §4.3）。

.. deprecated:: NPU 统一推理算力池 v2
    "保底/弹性/最大三层百分比配额"在新需求下被判定为"配额对象错了"（v2 §10）：
    真正要调的是"在线副本数"，由 autoscaler 按实时负载决定，而非业务百分比。
    新实现见 ``inference_pool.card_pool``（卡角色状态机）与 ``inference_pool.autoscaler``。

把总卡池切成三层，并按「租户已用 vs 卡级配额」给离线 worker 动态打 QoS 标签：

    总卡池
    ├─ 在线保障池      在线峰值预留(+排空余量)     GUARANTEED   全价   不可抢
    ├─ 离线保障配额池  Σ各租户卡级保障配额          PROTECTED    全价   不可抢(在线也不抢)
    └─ 弹性/突发池     其余 = 离线超配额部分        PREEMPTIBLE  折价   先被抢、也填谷

硬约束：在线保障池 + Σ离线保障配额 ≤ 总卡 —— 保证在线总能扩到峰值，
而离线在配额内有「不被早高峰打扰」的保障；只有超配额的突发部分参与抢占与填谷。

打标策略（落地要点）：worker 按该租户「已用是否超其卡配额」动态打标——
配额内整段 → PROTECTED（受保护），超出部分 → PREEMPTIBLE（进抢占候选）。
`preemption.select_victims` 逻辑不变（只挑 PREEMPTIBLE），变的就是这里的打标。
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from ..models import QoS


class PoolConstraintError(Exception):
    """违反 在线保障池 + Σ离线保障配额 ≤ 总卡。"""


@dataclass(frozen=True)
class PoolPlan:
    """一个机房（地域）的三池容量规划。"""

    total_cards: int
    online_reserved: int            # 在线保障池
    protected_quota_total: int      # Σ 各租户离线保障配额

    @property
    def burst_pool(self) -> int:
        """弹性/突发池 = 总卡 - 在线保障池 - 离线保障配额池。"""
        return self.total_cards - self.online_reserved - self.protected_quota_total

    def validate(self) -> None:
        if self.online_reserved < 0 or self.protected_quota_total < 0 or self.total_cards < 0:
            raise PoolConstraintError("卡数不能为负")
        if self.online_reserved + self.protected_quota_total > self.total_cards:
            raise PoolConstraintError(
                f"在线保障池({self.online_reserved}) + Σ离线保障配额"
                f"({self.protected_quota_total}) = "
                f"{self.online_reserved + self.protected_quota_total} > 总卡({self.total_cards})")


def online_reserved_with_drain(
    online_peak_cards: int,
    ramp_cards_per_second: float,
    drain_seconds: int,
) -> int:
    """在线保障池 = 在线峰值 + 排空余量（review 🟢5）。

    在线瞬时拉升要等被抢离线在 batch 边界优雅退出（drain_seconds），这段窗口在线
    须自有缓冲，否则违反在线 SLA（需求目标②硬约束）。余量 = 峰值爬升斜率 × 排空时延。
    """
    if online_peak_cards < 0 or ramp_cards_per_second < 0 or drain_seconds < 0:
        raise ValueError("参数不能为负")
    headroom = math.ceil(ramp_cards_per_second * drain_seconds)
    return online_peak_cards + headroom


def plan_pools(
    total_cards: int,
    online_reserved: int,
    tenant_quotas: dict[str, int],
) -> PoolPlan:
    """构建并校验三池规划。tenant_quotas = 各租户卡级保障配额。"""
    plan = PoolPlan(
        total_cards=total_cards,
        online_reserved=online_reserved,
        protected_quota_total=sum(tenant_quotas.values()),
    )
    plan.validate()
    return plan


def classify_cards(used_before: int, add_cards: int, tenant_quota: int) -> tuple[int, int]:
    """按卡粒度拆分新增卡：配额内算 PROTECTED，超出算 PREEMPTIBLE。

    返回 (protected_cards, preemptible_cards)。used_before = 该租户当前已占卡。
    """
    if add_cards < 0 or used_before < 0 or tenant_quota < 0:
        raise ValueError("参数不能为负")
    protected = max(0, min(add_cards, tenant_quota - used_before))
    preemptible = add_cards - protected
    return protected, preemptible


def assign_worker_qos(used_before: int, worker_cards: int, tenant_quota: int) -> QoS:
    """整 worker 粒度打标：worker 完全落在配额内 → PROTECTED，否则 → PREEMPTIBLE。

    跨界 worker（部分在配额内、部分超出）从严按突发处理——保证「在线只抢突发池」
    时不会误伤配额内容量，配额内永远是整 worker 受保护。
    """
    if worker_cards <= 0:
        raise ValueError("worker_cards 必须为正")
    if used_before + worker_cards <= tenant_quota:
        return QoS.PROTECTED
    return QoS.PREEMPTIBLE


def tag_workers(
    workers: list[tuple[str, str, int]],
    tenant_quotas: dict[str, int],
) -> dict[str, QoS]:
    """给一批运行中 worker 打标。

    workers = [(worker_id, tenant, cards), ...]（按希望保护的优先级排序，先到先占配额）。
    返回 {worker_id: QoS}。同租户按出现顺序累计占用，先进配额者受保护。
    """
    used: dict[str, int] = {}
    out: dict[str, QoS] = {}
    for worker_id, tenant, cards in workers:
        before = used.get(tenant, 0)
        quota = tenant_quotas.get(tenant, 0)
        out[worker_id] = assign_worker_qos(before, cards, quota)
        used[tenant] = before + cards
    return out
