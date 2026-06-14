"""抢占 victim 选择（架构 §4.3，决策三 / 三池模型）。

早高峰在线扩容需要卡时，**只从弹性/突发池（Preemptible）** 选牺牲者：
- 只选 Preemptible（GUARANTEED 在线、PROTECTED 离线配额内、BEST_EFFORT_FIXED 固定池都不抢）
- 优先杀小实例（碎卡好回收、加载便宜）
- 同等规模下优先杀加载已久的（浪费小；保护刚加载完的大实例）
凑够 cards_needed 即停。

Preemptible 标签由 `pools.tag_workers` 按「租户已用是否超卡配额」动态打：配额内 worker
是 PROTECTED 不会进候选，超配额 worker 才是 PREEMPTIBLE——故「在线只抢超配额」天然成立。
"""
from __future__ import annotations

from dataclasses import dataclass

from ..models import QoS


@dataclass
class RunningWorker:
    worker_id: str
    qos: QoS
    cards: int
    loaded_seconds_ago: float  # 已加载多久（越大=越老=浪费越小）


def select_victims(workers: list[RunningWorker], cards_needed: int) -> list[str]:
    """返回被抢占的 worker_id 列表，使释放卡数 ≥ cards_needed。

    只动弹性/突发池；若突发池卡数不足以凑够（说明在线峰值已吃进保障池余量，
    属容量规划问题，见 pools.online_reserved_with_drain），返回突发池能凑到的最大集合。
    """
    if cards_needed <= 0:
        return []
    candidates = [w for w in workers if w.qos == QoS.PREEMPTIBLE]
    # 先小后大；同规模老的先走
    candidates.sort(key=lambda w: (w.cards, -w.loaded_seconds_ago))
    freed = 0
    chosen: list[str] = []
    for w in candidates:
        if freed >= cards_needed:
            break
        chosen.append(w.worker_id)
        freed += w.cards
    return chosen
