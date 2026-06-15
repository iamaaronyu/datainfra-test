"""多模型离线编排（§2 第三层 / §6.3）。

其余 ~1.2 万卡按需装载 Qwen3.5VL / gpt-oss / MinerU 等跑幂等分片任务。复用
compute_platform 既有的**幂等分片队列**（这是旧实现里唯一与新需求方向一致、
可直接复用的资产，对应 R3.3），在其上补齐新需求要求的：

  R3.1  按需把卡装载为指定模型副本。
  R3.2  "换装" = 卸载旧 + 加载新（分钟级），支持 drain。
  R3.3  幂等分片：被中断 → 分片回队列 → 换机从未完成分片续跑（**无 checkpoint**）。
  R3.4  按模型聚合换装、按卡型成组，降低换装次数与碎片。
  R3.5  任务声明 模型/卡型/卡数/分片数/(可选)deadline。
  §5.8  deadline 任务仅在离线池内按紧迫度竞争，**绝不侵占在线副本**。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from compute_platform.models import Shard, ShardStatus
from compute_platform.queue.base import ShardQueue

from .card_pool import CardPool
from .models import Card, CardRole, CardType, ModelSpec, ShardTask, ONLINE_MODEL


def make_shards(task: ShardTask) -> list[Shard]:
    """为离线任务生成幂等分片（确定性 shard_id，可重入入队，R3.3）。"""
    return [
        Shard(
            shard_id=f"{task.job_id}-{i:06d}",
            job_id=task.job_id,
            model=task.model,
            file=f"{task.job_id}.jsonl",
            start_offset=i,
            end_offset=i + 1,
            rows=1,
            status=ShardStatus.PENDING,
        )
        for i in range(task.total_shards)
    ]


class OfflinePool:
    def __init__(
        self,
        pool: CardPool,
        queue: ShardQueue,
        specs: dict[str, ModelSpec],
        lease_seconds: int = 30,
    ):
        self.pool = pool
        self.queue = queue
        self.specs = specs
        self.lease_seconds = lease_seconds
        self.tasks: dict[str, ShardTask] = {}
        # card_id -> (shard_id, lease_token)：该卡当前在跑的分片。
        self._inflight: dict[str, tuple[str, str]] = {}

    # ---- 提交与编排 -----------------------------------------------------
    def submit(self, task: ShardTask) -> int:
        """登记任务并幂等入队其分片。返回新入队分片数。"""
        self.tasks[task.job_id] = task
        return self.queue.enqueue_many(make_shards(task))

    def place(self, model: str, want_cards: int) -> list[Card]:
        """为某模型装载副本：按卡型成组、整机聚合换装（R3.4/R5.2）。

        优先用首选卡型的空闲卡；强依赖卡型不可降级时只用该卡型。
        """
        spec = self.specs[model]
        idle = self.pool.cards_by_role(CardRole.OFFLINE)

        def card_pref(c: Card) -> int:
            # 0 = 完全匹配首选卡型（成组优先），1 = 其它。
            if spec.required_card_type is None:
                return 0
            return 0 if c.card_type == spec.required_card_type else 1

        # 已是该模型的卡免换装，排最前。
        idle.sort(key=lambda c: (c.model != model, card_pref(c), c.card_id))

        chosen: list[Card] = []
        for c in idle:
            if len(chosen) >= want_cards:
                break
            if spec.required_card_type and not spec.degradable:
                if c.card_type != spec.required_card_type:
                    continue
            chosen.append(c)
            if c.model != model:
                self.pool.reload_to(c.card_id, model, CardRole.OFFLINE)
        return chosen

    # ---- worker 循环（幂等分片）----------------------------------------
    def dispatch(self, card: Card) -> Shard | None:
        """让一张离线卡领取一个本模型分片。None = 该模型没活了。"""
        if card.role != CardRole.OFFLINE or card.model is None:
            return None
        shard = self.queue.claim(card.model, self.lease_seconds)
        if shard is not None:
            self._inflight[card.card_id] = (shard.shard_id, shard.lease_owner)
        return shard

    def complete(self, card: Card) -> bool:
        """提交该卡在跑的分片。"""
        rec = self._inflight.pop(card.card_id, None)
        if rec is None:
            return False
        shard_id, token = rec
        return self.queue.commit(shard_id, token)

    # ---- drain / 被中断续跑（§5.7）------------------------------------
    def drain_card(self, card: Card) -> str | None:
        """drain 一张卡：在途分片**回到队列**（不计重试），换机可续跑（R3.3）。

        返回被退回的 shard_id（无在途则 None）。这就是取代 checkpoint 状态机的机制：
        无需保存模型态，只要"结果写入幂等、分片可重入"。
        """
        rec = self._inflight.pop(card.card_id, None)
        if rec is None:
            return None
        shard_id, token = rec
        self.queue.abort(shard_id, token)  # 回到 PENDING，等待换机续跑
        return shard_id

    def reclaim_for_online(self, card_ids: list[str]) -> list[str]:
        """为在线扩容回收一批离线卡：先 drain 其在途分片（保证不丢、可续跑），

        再交由 CardPool 换装为在线副本。返回被退回队列的 shard_id 列表。
        """
        returned: list[str] = []
        for cid in card_ids:
            card = self.pool._cards.get(cid)
            if card is None:
                continue
            sid = self.drain_card(card)
            if sid is not None:
                returned.append(sid)
        return returned

    # ---- deadline 调度（§5.8，仅离线池内竞争）-------------------------
    def urgent_jobs(self, now: float) -> list[str]:
        """按 deadline 紧迫度排序的任务（无 deadline 排最后）。绝不侵占在线。"""
        def key(jid: str):
            d = self.tasks[jid].deadline
            return (d is None, d if d is not None else 0.0)

        return sorted(self.tasks.keys(), key=key)

    def is_finished(self, job_id: str) -> bool:
        return self.queue.stats(job_id).finished
