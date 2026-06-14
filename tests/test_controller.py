from compute_platform.models import CardType, ModelSpec
from compute_platform.scheduler import PoolDemand, desired_workers, should_add_big_worker

# 小模型：加载快、吞吐高
SMALL = ModelSpec("small-7B", CardType.B4, cards_per_worker=1,
                  load_seconds=30, throughput_rows_per_sec=120)
# 大模型：加载慢
BIG = ModelSpec("235B", CardType.B3, cards_per_worker=8,
                load_seconds=900, throughput_rows_per_sec=20)


def test_zero_backlog_zero_workers():
    d = PoolDemand(backlog_rows=0, free_cards=100, quota_cards=100)
    assert desired_workers(d, SMALL, k=5) == 0


def test_bounded_by_supply():
    # 海量积压，但只有 4 张空闲卡，small 占 1 卡 -> 最多 4
    d = PoolDemand(backlog_rows=10_000_000, free_cards=4, quota_cards=100,
                   target_runtime_seconds=600)
    assert desired_workers(d, SMALL, k=1) == 4


def test_bounded_by_quota():
    d = PoolDemand(backlog_rows=10_000_000, free_cards=100, quota_cards=3,
                   target_runtime_seconds=600)
    assert desired_workers(d, SMALL, k=1) == 3


def test_bounded_by_backlog():
    # 积压适中：work_seconds = 144000/120 = 1200；600s 窗口 -> by_backlog=2
    # 供给/配额都富余(100)，加载门槛 max=1200//30=40 不卡 -> 由积压封顶为 2
    d = PoolDemand(backlog_rows=144_000, free_cards=100, quota_cards=100,
                   target_runtime_seconds=600)
    assert desired_workers(d, SMALL, k=1) == 2


def test_load_cost_threshold_blocks_big_for_small_tail():
    # 大模型加载 900s，K=5 -> 需要 ≥4500 worker-seconds 的活才值得起一个
    # 积压 20*1000=20000 行 / 20 = 1000 worker-seconds < 4500 -> 不该扩
    d = PoolDemand(backlog_rows=20_000, free_cards=80, quota_cards=80,
                   target_runtime_seconds=600)
    assert desired_workers(d, BIG, k=5) == 0


def test_load_cost_threshold_allows_big_for_large_work():
    # 积压 200000 行 / 20 = 10000 worker-seconds > 4500 -> 可扩
    d = PoolDemand(backlog_rows=200_000, free_cards=80, quota_cards=80,
                   target_runtime_seconds=600)
    assert desired_workers(d, BIG, k=5) >= 1


def test_last_bus_blocks_big_worker_before_peak():
    # 现在 t=100，预测早高峰 t=1000；大模型加载 900s，N=1
    # 100 + 1*900 = 1000 不 < 1000 -> 不该起
    assert should_add_big_worker(100, 1000, BIG, n=1) is False
    # 提前量足够则放行
    assert should_add_big_worker(50, 2000, BIG, n=1) is True


def test_last_bus_no_predicted_preempt():
    # 深夜无预测抢占 -> 放行
    assert should_add_big_worker(100, None, BIG, n=1) is True
