"""在线副本自动扩缩：负载驱动、预热下限、缩容迟滞（§6.2）。"""
from inference_pool import Autoscaler, AutoscalerConfig, PrewarmWindow
from inference_pool.models import LoadMetrics


def _scaler(clock):
    cfg = AutoscalerConfig(min_cards=64, max_cards=3000, qps_per_card=2.0,
                           headroom=0.0, scale_down_cooldown=300.0, scale_step_down=0.5)
    return Autoscaler(cfg, clock=clock)


def test_scales_up_with_qps(clock):
    a = _scaler(clock)
    # 2000 QPS / 2 per card = 1000 卡。
    t = a.compute_target(LoadMetrics(qps=2000), now=0)
    assert t == 1000


def test_clamped_to_bounds(clock):
    a = _scaler(clock)
    assert a.compute_target(LoadMetrics(qps=0), now=0) == 64       # 下限
    assert a.compute_target(LoadMetrics(qps=100000), now=0) == 3000  # 上限


def test_queue_depth_drives_scale(clock):
    a = _scaler(clock)
    # 队列深度也能推高目标（即便 qps 读数偏低）。
    t = a.compute_target(LoadMetrics(qps=10, queue_depth=8000), now=0)
    assert t > 1000


def test_ttft_breach_adds_replicas(clock):
    a = _scaler(clock)
    base = a.compute_target(LoadMetrics(qps=1000, ttft_p95=1.0), now=0)
    a2 = _scaler(clock)
    breach = a2.compute_target(LoadMetrics(qps=1000, ttft_p95=2.0), now=0)
    assert breach > base


def test_scale_up_is_immediate(clock):
    a = _scaler(clock)
    a.compute_target(LoadMetrics(qps=200), now=0)   # 100 卡
    t = a.compute_target(LoadMetrics(qps=2000), now=1)  # 立刻跳到 1000
    assert t == 1000


def test_scale_down_hysteresis(clock):
    """缩容需持续低于冷却时长才执行；短暂下探不缩（R2.4 防抖）。"""
    a = _scaler(clock)
    a.compute_target(LoadMetrics(qps=4000), now=0)   # 2000 卡
    # 负载骤降，但冷却(300s)未到 → 维持。
    t1 = a.compute_target(LoadMetrics(qps=200), now=10)
    assert t1 == 2000
    # 过了冷却 → 开始缩，但单步只回收一半差额（迟滞）。
    t2 = a.compute_target(LoadMetrics(qps=200), now=400)
    assert 100 < t2 < 2000


def test_predictive_prewarm_floor(clock):
    """早高峰前预热窗口抬高在线下限，避免冷启动落在堆积窗口（R2.3）。"""
    a = _scaler(clock)
    a.add_prewarm(PrewarmWindow(start=100, end=200, floor_cards=800))
    # 窗口内即便 QPS 低，也维持预热下限。
    assert a.compute_target(LoadMetrics(qps=0), now=150) == 800
    # 窗口外（用全新 scaler 隔离缩容迟滞）不再有预热下限，回到 min。
    fresh = _scaler(clock)
    fresh.add_prewarm(PrewarmWindow(start=100, end=200, floor_cards=800))
    assert fresh.compute_target(LoadMetrics(qps=0), now=900) == 64
