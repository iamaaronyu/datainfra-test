from compute_platform.models import QoS
from compute_platform.scheduler import RunningWorker, select_victims


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
