"""多模型离线编排：幂等分片、换装聚合、drain 续跑、deadline（§6.3 / §5.7-5.8）。"""
from inference_pool import Card, CardRole, CardType, ShardTask
from inference_pool.models import ONLINE_MODEL


def _ready(pool, seconds=200):
    pool.clock.advance(seconds)
    pool.tick()


def test_submit_is_idempotent(offline):
    task = ShardTask(job_id="j1", model="Qwen3.5VL", cards=4, total_shards=10)
    assert offline.submit(task) == 10
    assert offline.submit(task) == 0  # 重复入队去重（R3.3）


def test_place_prefers_required_card_type(offline, pool):
    offline.submit(ShardTask(job_id="j", model="MinerU", cards=4, total_shards=5))
    cards = offline.place("MinerU", 4)  # MinerU 强依赖 B3、不可降级
    _ready(pool)
    for c in cards:
        assert c.card_type == CardType.B3
        assert c.model == "MinerU"


def test_dispatch_complete_drains_queue(offline, pool):
    offline.submit(ShardTask(job_id="j", model="Qwen3.5VL", cards=2, total_shards=6))
    cards = offline.place("Qwen3.5VL", 2)
    _ready(pool)
    done = 0
    # 轮流派发+提交，直到队列清空。
    for _ in range(100):
        progressed = False
        for c in cards:
            if offline.dispatch(c) is not None:
                assert offline.complete(c)
                done += 1
                progressed = True
        if not progressed:
            break
    assert done == 6
    assert offline.is_finished("j")


def test_drain_returns_inflight_shard_for_resume(offline, pool):
    """被中断的分片回到队列，换机续跑（取代 checkpoint，§5.7 / R3.3）。"""
    offline.submit(ShardTask(job_id="j", model="gpt-oss", cards=1, total_shards=3))
    cards = offline.place("gpt-oss", 1)
    _ready(pool)
    card = cards[0]
    shard = offline.dispatch(card)
    assert shard is not None
    # 模拟早高峰回收该卡：drain，在途分片回队列。
    returned = offline.drain_card(card)
    assert returned == shard.shard_id
    # 另一张卡可续跑同一分片（未丢失）。
    other = offline.place("gpt-oss", 1)
    _ready(pool)
    resumed = offline.dispatch(other[0])
    assert resumed is not None
    assert resumed.shard_id == shard.shard_id


def test_reclaim_for_online_drains_and_returns_shards(offline, pool):
    offline.submit(ShardTask(job_id="j", model="Qwen3.5VL", cards=3, total_shards=9))
    cards = offline.place("Qwen3.5VL", 3)
    _ready(pool)
    for c in cards:
        offline.dispatch(c)
    returned = offline.reclaim_for_online([c.card_id for c in cards])
    assert len(returned) == 3  # 三个在途分片都被安全退回


def test_deadline_jobs_ordered_by_urgency(offline):
    offline.submit(ShardTask(job_id="late", model="gpt-oss", cards=1, total_shards=1, deadline=900))
    offline.submit(ShardTask(job_id="soon", model="gpt-oss", cards=1, total_shards=1, deadline=100))
    offline.submit(ShardTask(job_id="none", model="gpt-oss", cards=1, total_shards=1))
    order = offline.urgent_jobs(now=0)
    assert order == ["soon", "late", "none"]
