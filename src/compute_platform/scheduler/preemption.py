"""抢占 victim 选择（§7.2）。

早高峰在线扩容需要卡时，从可抢占（Preemptible）worker 中选牺牲者：
- 只选 Preemptible（Guaranteed 在线、BestEffort-Fixed 固定池都不抢）
- 优先杀小实例（碎卡好回收、加载便宜）
- 同等规模下优先杀加载已久的（浪费小；保护刚加载完的大实例）
凑够 cards_needed 即停。
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

    若可抢占资源不足以凑够，返回能凑到的最大集合（在线优先，尽力让路）。
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
