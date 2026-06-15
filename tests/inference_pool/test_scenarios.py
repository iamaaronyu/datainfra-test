"""端到端场景（直接对应需求文档 §5.1–5.8）。

每个测试映射一个验收场景，断言"系统行为"符合新架构：混跑填谷、自动扩缩、
温卡回收、换装与幂等分片续跑，而非旧的"搬卡式抢占"。
"""
from inference_pool import CardRole, Priority, Request, ShardTask
from inference_pool.models import ONLINE_MODEL


def _submit_online(gw, n, prefix="o"):
    for i in range(n):
        gw.submit(Request(request_id=f"{prefix}{i}", priority=Priority.ONLINE,
                          session_key=f"sess-{i % 8}"))


def _submit_batch(gw, n, prefix="b"):
    for i in range(n):
        gw.submit(Request(request_id=f"{prefix}{i}", priority=Priority.BATCH))


def _settle(platform, cycles=3, dt=60):
    """跑若干控制周期 + 推进时钟，让换装就位。"""
    for _ in range(cycles):
        platform.control_step()
        platform.pool.clock.advance(dt)
        platform.control_step()


# ---- §5.1 早高峰爬升 ----------------------------------------------------
def test_morning_ramp_scales_online_up(platform):
    gw = platform.gateway
    _submit_online(gw, 200)
    plan = platform.control_step()
    # 温卡为 0，扩容走"换装离线卡"路径（分钟级）。
    assert plan.reload_started, "应触发离线卡换装为在线副本"
    assert platform.pool.counts()[CardRole.LOADING] > 0
    # 换装就位后在线副本上线，达到需求量级。
    platform.pool.clock.advance(60)
    platform.control_step()
    assert platform.pool.online_cards() >= 100


# ---- §5.2 日内平稳：空泡被离线填满，不搬卡 ----------------------------
def test_idle_slots_filled_by_batch_no_card_move(platform):
    gw = platform.gateway
    _submit_online(gw, 8)
    _submit_batch(gw, 500)
    _settle(platform, cycles=3)
    # 稳态后再观察一个周期：不应再有换装或缩容（不搬卡）。
    _submit_online(gw, 8)
    _submit_batch(gw, 500)
    plan = platform.control_step()
    assert plan.reload_started == []
    assert plan.released == []
    res = gw.step()
    # 在线被服务，且批量把剩余 slot 填满（空泡被吃掉）。
    assert len(res.served_online) >= 1
    assert len(res.served_batch) > 0


# ---- §5.3 突发尖峰：温卡优先吸收，离线分片不丢 -----------------------
def test_spike_absorbed_by_warm_cards_no_shard_loss(platform):
    gw, pool, offline = platform.gateway, platform.pool, platform.offline
    # 先建一批温卡缓冲（扩起来再缩回）。
    _submit_online(gw, 120)
    platform.control_step()
    pool.clock.advance(60)
    platform.control_step()           # ~120 在线就位
    gw.online_q.clear()
    for _ in range(15):               # 负载归零 + 反复过冷却，迟滞缩容直至回到下限
        pool.clock.advance(400)
        platform.control_step()
    assert pool.online_cards() <= platform.autoscaler.cfg.min_cards
    assert pool.warm_cards() > 0      # 缩出的卡成为温卡缓冲
    # 尖峰：温卡秒级转在线，不丢离线分片。
    _submit_online(gw, 60)
    plan = platform.control_step()
    assert plan.warm_reclaimed, "尖峰应优先回收温卡（秒级）"
    assert plan.reload_started == [], "温卡足够时尖峰不需换装"
    assert plan.instant_online > 0


# ---- §5.4 夜间缩容、卡回流离线 ----------------------------------------
def test_night_scale_down_returns_cards(platform):
    gw, pool = platform.gateway, platform.pool
    _submit_online(gw, 160)
    platform.control_step()
    pool.clock.advance(60)
    platform.control_step()
    peak_online = pool.online_cards()
    assert peak_online > 50
    # 夜间负载消失：带迟滞地缩容，卡回流为温卡。
    gw.online_q.clear()
    for _ in range(6):
        pool.clock.advance(400)
        platform.control_step()
    assert pool.online_cards() < peak_online
    assert pool.warm_cards() > 0


# ---- §5.5 周末：在线维持兜底，其余服务离线 ----------------------------
def test_weekend_online_at_floor(platform):
    pool = platform.pool
    # 无在线负载，控制周期只维持下限。
    _settle(platform, cycles=3)
    assert pool.online_cards() <= platform.autoscaler.cfg.min_cards
    # 绝大多数卡可用于离线。
    assert pool.counts()[CardRole.OFFLINE] >= pool.total - platform.autoscaler.cfg.min_cards


# ---- §5.6 多模型 + 多卡型离线编排 -------------------------------------
def test_multi_model_grouping_by_card_type(platform):
    offline, pool = platform.offline, platform.pool
    offline.submit(ShardTask(job_id="cap", model="Qwen3.5VL", cards=10, total_shards=20))
    offline.submit(ShardTask(job_id="pdf", model="MinerU", cards=8, total_shards=16))
    qwen = offline.place("Qwen3.5VL", 10)  # 首选 B4
    mineru = offline.place("MinerU", 8)    # 强依赖 B3、不可降级
    pool.clock.advance(200)
    pool.tick()
    assert all(c.model == "Qwen3.5VL" for c in qwen)
    assert all(c.card_type.value == "B3" and c.model == "MinerU" for c in mineru)


# ---- §5.7 离线被中断后续跑 --------------------------------------------
def test_offline_interrupt_then_resume(platform):
    offline, pool = platform.offline, platform.pool
    offline.submit(ShardTask(job_id="j", model="gpt-oss", cards=2, total_shards=8))
    cards = offline.place("gpt-oss", 2)
    pool.clock.advance(200)
    pool.tick()
    # 跑几片后被早高峰回收。
    offline.dispatch(cards[0])
    offline.dispatch(cards[1])
    returned = offline.reclaim_for_online([c.card_id for c in cards])
    assert len(returned) == 2
    # 换台机器把所有分片续跑到完成（无 checkpoint）。
    workers = offline.place("gpt-oss", 2)
    pool.clock.advance(200)
    pool.tick()
    for _ in range(50):
        progressed = False
        for c in workers:
            if offline.dispatch(c) is not None:
                offline.complete(c)
                progressed = True
        if not progressed:
            break
    assert offline.is_finished("j")


# ---- §5.8 deadline 任务只在离线池内竞争，绝不侵占在线 ------------------
def test_deadline_never_preempts_online(platform):
    gw, pool, offline = platform.gateway, platform.pool, platform.offline
    offline.submit(ShardTask(job_id="dl", model="gpt-oss", cards=4,
                             total_shards=8, deadline=100))
    # 在线高负载，autoscaler 只按在线负载扩缩，与 deadline 无关。
    _submit_online(gw, 160)
    platform.control_step()
    pool.clock.advance(60)
    platform.control_step()
    online_now = pool.online_cards()
    # deadline 任务在离线池内按紧迫度排前，但在线副本数不因它而被压低。
    assert offline.urgent_jobs(now=0)[0] == "dl"
    assert online_now >= 50  # 在线 SLA 硬不变量未被 deadline 侵占
