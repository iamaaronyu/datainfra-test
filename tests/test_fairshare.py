"""跨产线加权公平分配测试（架构 §5.2）。"""
from compute_platform.scheduler.fairshare import LineDemand, fair_share


def test_proportional_when_demand_unbounded():
    lines = [
        LineDemand("l1", weight=3.0, want=1000),
        LineDemand("l2", weight=1.0, want=1000),
    ]
    alloc = fair_share(400, lines)
    assert sum(alloc.values()) == 400
    assert alloc["l1"] == 300 and alloc["l2"] == 100  # 3:1


def test_capped_by_want_redistributes_leftover():
    # l1 权重高但只想要 50；剩余应流给 l2
    lines = [
        LineDemand("l1", weight=3.0, want=50),
        LineDemand("l2", weight=1.0, want=1000),
    ]
    alloc = fair_share(400, lines)
    assert alloc["l1"] == 50
    assert alloc["l2"] == 350
    assert sum(alloc.values()) == 400


def test_total_never_exceeds_free_or_demand():
    lines = [
        LineDemand("l1", weight=1.0, want=30),
        LineDemand("l2", weight=1.0, want=30),
    ]
    alloc = fair_share(400, lines)  # 总需求 60 < 400
    assert sum(alloc.values()) == 60
    assert alloc["l1"] == 30 and alloc["l2"] == 30


def test_integer_conservation_no_card_lost():
    lines = [
        LineDemand("l1", weight=1.0, want=1000),
        LineDemand("l2", weight=1.0, want=1000),
        LineDemand("l3", weight=1.0, want=1000),
    ]
    alloc = fair_share(100, lines)  # 100/3 不整除
    assert sum(alloc.values()) == 100   # 守恒，无丢卡
    assert max(alloc.values()) - min(alloc.values()) <= 1  # 尽量均匀


def test_zero_free_cards():
    lines = [LineDemand("l1", weight=1.0, want=10)]
    assert fair_share(0, lines) == {"l1": 0}


def test_zero_weight_line_gets_nothing():
    lines = [
        LineDemand("l1", weight=0.0, want=100),
        LineDemand("l2", weight=1.0, want=100),
    ]
    alloc = fair_share(50, lines)
    assert alloc["l1"] == 0
    assert alloc["l2"] == 50
