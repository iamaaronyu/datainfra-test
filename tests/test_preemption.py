from compute_platform.models import QoS
from compute_platform.scheduler import RunningWorker, select_victims, tag_workers


def test_never_preempts_guaranteed():
    workers = [
        RunningWorker("online-1", QoS.GUARANTEED, cards=4, loaded_seconds_ago=100),
        RunningWorker("online-2", QoS.GUARANTEED, cards=4, loaded_seconds_ago=100),
    ]
    assert select_victims(workers, cards_needed=4) == []


def test_never_preempts_fixed_pool():
    workers = [RunningWorker("fixed-1", QoS.BEST_EFFORT_FIXED, cards=8,
                             loaded_seconds_ago=10)]
    assert select_victims(workers, cards_needed=8) == []


def test_prefers_small_workers_first():
    workers = [
        RunningWorker("big", QoS.PREEMPTIBLE, cards=8, loaded_seconds_ago=10),
        RunningWorker("small-1", QoS.PREEMPTIBLE, cards=1, loaded_seconds_ago=10),
        RunningWorker("small-2", QoS.PREEMPTIBLE, cards=1, loaded_seconds_ago=10),
    ]
    victims = select_victims(workers, cards_needed=2)
    assert set(victims) == {"small-1", "small-2"}  # 不动大实例


def test_frees_enough_cards():
    workers = [
        RunningWorker("a", QoS.PREEMPTIBLE, cards=1, loaded_seconds_ago=10),
        RunningWorker("b", QoS.PREEMPTIBLE, cards=8, loaded_seconds_ago=10),
    ]
    victims = select_victims(workers, cards_needed=4)
    freed = sum(w.cards for w in workers if w.worker_id in victims)
    assert freed >= 4


def test_same_size_prefers_older():
    workers = [
        RunningWorker("fresh", QoS.PREEMPTIBLE, cards=8, loaded_seconds_ago=5),
        RunningWorker("old", QoS.PREEMPTIBLE, cards=8, loaded_seconds_ago=500),
    ]
    victims = select_victims(workers, cards_needed=8)
    assert victims == ["old"]  # 老的浪费小，先走


def test_best_effort_when_insufficient():
    workers = [RunningWorker("a", QoS.PREEMPTIBLE, cards=2, loaded_seconds_ago=10)]
    victims = select_victims(workers, cards_needed=10)
    assert victims == ["a"]  # 尽力让路，把能腾的都腾


def test_zero_need():
    workers = [RunningWorker("a", QoS.PREEMPTIBLE, cards=2, loaded_seconds_ago=10)]
    assert select_victims(workers, cards_needed=0) == []


# ---- 三池模型：只抢超配额（决策三，架构 §4.3） ----

def test_never_preempts_protected():
    # 离线配额内（PROTECTED）即使在线缺卡也不抢
    workers = [RunningWorker("prot-1", QoS.PROTECTED, cards=8, loaded_seconds_ago=10)]
    assert select_victims(workers, cards_needed=8) == []


def test_three_pool_only_over_quota_preempted():
    # team-a 配额 16：前两个 worker 受保护，第三个超配额可抢
    running = [
        ("w-prot-1", "team-a", 8),
        ("w-prot-2", "team-a", 8),
        ("w-burst-1", "team-a", 8),  # 超配额
    ]
    tags = tag_workers(running, {"team-a": 16})
    workers = [
        RunningWorker(wid, tags[wid], cards=8, loaded_seconds_ago=10)
        for wid, _, _ in running
    ]
    # 在线要 16 卡，但只能从突发池(1 个 8 卡 worker)拿到 8——配额内不让路
    victims = select_victims(workers, cards_needed=16)
    assert victims == ["w-burst-1"]
