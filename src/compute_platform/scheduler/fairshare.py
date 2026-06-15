"""跨产线加权公平分配（架构 §5.2）。

40 条产线竞争同一批空闲卡（弹性/突发池）时，按权重（= 配额 + 积压派生）加权分配，
且不超过各产线本轮真实需求（want）。剩余卡按权重做 max-min 注水二次分配，避免浪费。
纯函数，便于全量测试。

.. deprecated:: NPU 统一推理算力池 v2
    基于"业务百分比/产线权重"的弹性池二次分配在新需求下不再是调度核心。
    新实现以"各模型的在线副本数 + 离线分片队列"为调度对象，见 ``inference_pool``。
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LineDemand:
    line: str
    weight: float   # 加权份额（配额 + 积压派生），须 > 0 才参与
    want: int       # 本轮真实想要的卡数（受积压/配额上限约束）


def fair_share(free_cards: int, lines: list[LineDemand]) -> dict[str, int]:
    """把 free_cards 在各产线间加权公平分配。

    规则：
    1. 初始按 weight 比例分配，但每条不超过其 want（上限封顶）；
    2. 被 want 截断省下的卡，进入下一轮，仅在「还没吃饱」的产线间按权重再分；
    3. 迭代到无卡可分或所有产线都吃饱；整数分配用最大余额法保证总和守恒。
    返回 {line: cards}，Σ ≤ free_cards 且 ≤ Σwant。
    """
    alloc: dict[str, int] = {ln.line: 0 for ln in lines}
    if free_cards <= 0:
        return alloc

    remaining = free_cards
    active = [ln for ln in lines if ln.weight > 0 and ln.want > 0]

    while remaining > 0 and active:
        total_weight = sum(ln.weight for ln in active)
        # 本轮按权重的理想（浮点）份额
        ideal = {ln.line: remaining * ln.weight / total_weight for ln in active}
        # 先取整数下限，受「剩余 want」封顶
        granted_this_round: dict[str, int] = {}
        for ln in active:
            headroom = ln.want - alloc[ln.line]
            granted_this_round[ln.line] = min(int(ideal[ln.line]), headroom)
        used = sum(granted_this_round.values())
        leftover = remaining - used

        # 余数按「理想小数部分 + 仍有 headroom」最大者依次发放，保证守恒
        if leftover > 0:
            order = sorted(
                active,
                key=lambda ln: (ideal[ln.line] - int(ideal[ln.line])),
                reverse=True,
            )
            for ln in order:
                if leftover <= 0:
                    break
                if alloc[ln.line] + granted_this_round[ln.line] < ln.want:
                    granted_this_round[ln.line] += 1
                    leftover -= 1

        progressed = False
        for ln in active:
            g = granted_this_round[ln.line]
            if g > 0:
                alloc[ln.line] += g
                remaining -= g
                progressed = True

        # 移除已吃饱的产线
        active = [ln for ln in active if alloc[ln.line] < ln.want]
        if not progressed:
            break  # 防御：无法再推进（理论上不会发生）

    return alloc
