import pytest

from compute_platform.config import Config
from compute_platform.governance import MeteringService, QuotaManager, QuotaExceeded
from compute_platform.governance.metering import UsageEvent
from compute_platform.models import CardType, QoS


# ---- 配额 ----

def test_quota_reserve_and_release():
    q = QuotaManager({"team-a": 10})
    q.reserve("team-a", 4)
    assert q.used("team-a") == 4
    assert q.available("team-a") == 6
    q.release("team-a", 4)
    assert q.used("team-a") == 0


def test_quota_exceeded_raises():
    q = QuotaManager({"team-a": 8})
    q.reserve("team-a", 8)
    with pytest.raises(QuotaExceeded):
        q.reserve("team-a", 1)


def test_quota_try_reserve():
    q = QuotaManager({"team-a": 8})
    assert q.try_reserve("team-a", 8) is True
    assert q.try_reserve("team-a", 1) is False


def test_unknown_tenant_has_no_quota():
    q = QuotaManager({"team-a": 8})
    assert q.available("ghost") == 0
    assert q.try_reserve("ghost", 1) is False


# ---- 计量结算（价格杠杆） ----

def _config():
    return Config(base_price_per_card_hour=10.0, b3_price_factor=1.5,
                  b4_price_factor=1.0, preemptible_discount=0.4)


def test_guaranteed_full_price_b3():
    m = MeteringService(_config())
    # 8 卡 B3 跑 1 小时，Guaranteed 全价：8 * 10 * 1.5 * 1.0 = 120
    cost = m.cost_of(UsageEvent("online", CardType.B3, QoS.GUARANTEED, 8, 3600))
    assert cost == pytest.approx(120.0)


def test_protected_full_price_like_guaranteed():
    m = MeteringService(_config())
    # 离线配额内 PROTECTED 全价，与 Guaranteed 同价：8 * 10 * 1.5 = 120
    prot = m.cost_of(UsageEvent("batch", CardType.B3, QoS.PROTECTED, 8, 3600))
    guar = m.cost_of(UsageEvent("online", CardType.B3, QoS.GUARANTEED, 8, 3600))
    assert prot == pytest.approx(120.0)
    assert prot == pytest.approx(guar)


def test_protected_more_expensive_than_preemptible():
    m = MeteringService(_config())
    prot = m.cost_of(UsageEvent("b", CardType.B3, QoS.PROTECTED, 8, 3600))
    burst = m.cost_of(UsageEvent("b", CardType.B3, QoS.PREEMPTIBLE, 8, 3600))
    assert burst < prot  # 价格杠杆：突发池更便宜，激励把活推进突发池


def test_preemptible_discounted():
    m = MeteringService(_config())
    # 8 卡 B3 1 小时，可抢占 4 折：120 * 0.4 = 48
    cost = m.cost_of(UsageEvent("batch", CardType.B3, QoS.PREEMPTIBLE, 8, 3600))
    assert cost == pytest.approx(48.0)


def test_b4_cheaper_than_b3():
    m = MeteringService(_config())
    b3 = m.cost_of(UsageEvent("t", CardType.B3, QoS.GUARANTEED, 1, 3600))
    b4 = m.cost_of(UsageEvent("t", CardType.B4, QoS.GUARANTEED, 1, 3600))
    assert b4 < b3


def test_preempted_time_not_billed():
    m = MeteringService(_config())
    # 跑 3600s，被抢占浪费 1800s，只计 1800s
    full = m.cost_of(UsageEvent("b", CardType.B3, QoS.PREEMPTIBLE, 8, 3600, 0))
    half = m.cost_of(UsageEvent("b", CardType.B3, QoS.PREEMPTIBLE, 8, 3600, 1800))
    assert half == pytest.approx(full / 2)


def test_bill_accumulates_per_tenant():
    m = MeteringService(_config())
    m.record(UsageEvent("a", CardType.B3, QoS.GUARANTEED, 1, 3600))
    m.record(UsageEvent("a", CardType.B4, QoS.PREEMPTIBLE, 1, 3600))
    m.record(UsageEvent("b", CardType.B3, QoS.GUARANTEED, 1, 3600))
    bill = m.bill()
    assert set(bill) == {"a", "b"}
    assert bill["a"] == pytest.approx(15.0 + 4.0)  # 15 + (10*1*0.4)
    assert bill["b"] == pytest.approx(15.0)
