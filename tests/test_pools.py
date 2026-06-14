"""三池模型测试（架构 §4.3，决策三）。"""
import pytest

from compute_platform.models import QoS
from compute_platform.scheduler.pools import (
    PoolPlan,
    PoolConstraintError,
    plan_pools,
    classify_cards,
    assign_worker_qos,
    tag_workers,
    online_reserved_with_drain,
)


def test_plan_pools_burst_is_remainder():
    plan = plan_pools(5000, 3000, {"a": 800, "b": 700})
    assert plan.protected_quota_total == 1500
    assert plan.burst_pool == 5000 - 3000 - 1500  # 500


def test_constraint_violation_raises():
    # 在线保障池 + Σ配额 > 总卡
    with pytest.raises(PoolConstraintError):
        plan_pools(5000, 3000, {"a": 1500, "b": 1000})


def test_constraint_exactly_full_is_ok_zero_burst():
    plan = plan_pools(5000, 3000, {"a": 2000})
    assert plan.burst_pool == 0
    plan.validate()  # 不抛


def test_classify_cards_within_quota_all_protected():
    prot, preempt = classify_cards(used_before=0, add_cards=8, tenant_quota=16)
    assert (prot, preempt) == (8, 0)


def test_classify_cards_straddles_quota():
    # 已用 12，再要 8，配额 16 → 4 张进配额(protected)、4 张超配(preemptible)
    prot, preempt = classify_cards(used_before=12, add_cards=8, tenant_quota=16)
    assert (prot, preempt) == (4, 4)


def test_classify_cards_fully_over_quota():
    prot, preempt = classify_cards(used_before=16, add_cards=8, tenant_quota=16)
    assert (prot, preempt) == (0, 8)


def test_assign_worker_qos_protected_when_inside_quota():
    assert assign_worker_qos(used_before=0, worker_cards=8, tenant_quota=16) == QoS.PROTECTED
    assert assign_worker_qos(used_before=8, worker_cards=8, tenant_quota=16) == QoS.PROTECTED


def test_assign_worker_qos_preemptible_when_straddles_or_over():
    # 跨界从严按突发
    assert assign_worker_qos(used_before=12, worker_cards=8, tenant_quota=16) == QoS.PREEMPTIBLE
    assert assign_worker_qos(used_before=16, worker_cards=8, tenant_quota=16) == QoS.PREEMPTIBLE


def test_tag_workers_first_come_protected_then_burst():
    workers = [
        ("w1", "team-a", 8),   # 0→8  ≤16 protected
        ("w2", "team-a", 8),   # 8→16 ≤16 protected
        ("w3", "team-a", 8),   # 16→24 >16 preemptible
        ("w4", "team-b", 8),   # team-b 配额 0 → preemptible
    ]
    tags = tag_workers(workers, {"team-a": 16, "team-b": 0})
    assert tags == {
        "w1": QoS.PROTECTED,
        "w2": QoS.PROTECTED,
        "w3": QoS.PREEMPTIBLE,
        "w4": QoS.PREEMPTIBLE,
    }


def test_online_reserved_includes_drain_headroom():
    # 峰值 2900，爬升 0.5 卡/秒，排空 120s → 余量 ceil(60)=60 → 2960
    assert online_reserved_with_drain(2900, 0.5, 120) == 2960
    # 无爬升 → 无余量
    assert online_reserved_with_drain(3000, 0.0, 120) == 3000


def test_pool_plan_negative_rejected():
    with pytest.raises(PoolConstraintError):
        PoolPlan(total_cards=-1, online_reserved=0, protected_quota_total=0).validate()
