"""卡池状态机：温卡优先回收、换装时序、缩容回流（§3 / §5）。"""
from inference_pool import CardRole, ONLINE_MODEL


def _make_warm(pool, n):
    """造 n 张温卡：先扩在线再缩回，留下保留 GLM5.1 权重的温卡。"""
    pool.scale_online(n)            # 从离线 reload
    pool.clock.advance(60)
    pool.tick()
    pool.scale_online(0)           # 缩回 → 这些卡变温卡
    assert pool.warm_cards() == n


def test_warm_first_reclaim_is_instant(pool):
    """扩容优先回收温卡，秒级到位、不进入 LOADING（R2.2）。"""
    _make_warm(pool, 10)
    plan = pool.scale_online(8)
    assert len(plan.warm_reclaimed) == 8
    assert plan.reload_started == []
    # 立即可服务 = 温卡秒级转在线。
    assert plan.instant_online == 8
    assert pool.online_cards() == 8


def test_reload_when_warm_insufficient(pool):
    """温卡不足才 reload 离线卡，且需等 load_seconds 后才上线（分钟级）。"""
    _make_warm(pool, 5)
    plan = pool.scale_online(20)
    assert len(plan.warm_reclaimed) == 5
    assert len(plan.reload_started) == 15
    # 换装中的卡还没上线。
    assert pool.online_cards() == 5
    assert pool.counts()[CardRole.LOADING] == 15
    pool.clock.advance(60)
    pool.tick()
    assert pool.online_cards() == 20


def test_scale_down_returns_cards_as_warm(pool):
    """缩容把在线卡退为温卡（保留权重，作温卡缓冲），不是直接 OFFLINE（§5.4/§5.3）。"""
    pool.scale_online(30)
    pool.clock.advance(60)
    pool.tick()
    plan = pool.scale_online(10)
    assert len(plan.released) == 20
    assert pool.online_cards() == 10
    assert pool.warm_cards() == 20


def test_shortfall_when_pool_exhausted(pool):
    """目标超过全池可用，记录缺口而非崩溃。"""
    plan = pool.scale_online(pool.total + 50)
    assert plan.shortfall == 50


def test_online_invariant_never_below_target_until_ticked(pool):
    """换装中绝不假装在线已就位（在线 SLA 硬不变量，§3.1）。"""
    pool.scale_online(40)
    # 还没 tick：在线卡数仍为 0，gateway 不会被喂超过实际可用的卡。
    assert pool.online_cards() == 0
