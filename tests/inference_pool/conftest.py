"""inference_pool 测试夹具。"""
import pytest

from inference_pool import (
    Autoscaler,
    AutoscalerConfig,
    CacheAffinityRouter,
    Card,
    CardPool,
    CardType,
    InferenceGateway,
    ModelSpec,
    ONLINE_MODEL,
    OfflinePool,
    Platform,
)
from compute_platform.queue import SqliteShardQueue


class FakeClock:
    """可注入的逻辑时钟，测试无需真实 sleep。"""

    def __init__(self, t: float = 0.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> float:
        self.t += dt
        return self.t


def default_specs() -> dict[str, ModelSpec]:
    return {
        ONLINE_MODEL: ModelSpec(ONLINE_MODEL, CardType.B3, degradable=True, load_seconds=60.0,
                                throughput_rows_per_sec=5.0),
        "Qwen3.5VL": ModelSpec("Qwen3.5VL", CardType.B4, degradable=True, load_seconds=120.0,
                               throughput_rows_per_sec=3.0),
        "gpt-oss": ModelSpec("gpt-oss", CardType.B4, degradable=True, load_seconds=90.0,
                             throughput_rows_per_sec=4.0),
        "MinerU": ModelSpec("MinerU", CardType.B3, degradable=False, load_seconds=60.0,
                            throughput_rows_per_sec=8.0),
    }


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def specs() -> dict[str, ModelSpec]:
    return default_specs()


def build_cards(n_b3: int, n_b4: int):
    cards = []
    for i in range(n_b3):
        cards.append(Card(card_id=f"b3-{i:05d}", card_type=CardType.B3))
    for i in range(n_b4):
        cards.append(Card(card_id=f"b4-{i:05d}", card_type=CardType.B4))
    return cards


@pytest.fixture
def pool(specs, clock) -> CardPool:
    # 小规模便于断言：100 张 B3 + 100 张 B4（语义同 1.5 万卡，按比例）。
    return CardPool(build_cards(100, 100), specs, clock=clock)


@pytest.fixture
def gateway() -> InferenceGateway:
    return InferenceGateway(slots_per_card=4, router=CacheAffinityRouter())


@pytest.fixture
def autoscaler(clock) -> Autoscaler:
    cfg = AutoscalerConfig(min_cards=4, max_cards=180, qps_per_card=2.0,
                           scale_down_cooldown=300.0)
    return Autoscaler(cfg, clock=clock)


@pytest.fixture
def queue(tmp_path) -> SqliteShardQueue:
    return SqliteShardQueue(str(tmp_path / "shards.db"), max_retries=3)


@pytest.fixture
def offline(pool, queue, specs) -> OfflinePool:
    return OfflinePool(pool, queue, specs, lease_seconds=30)


@pytest.fixture
def platform(pool, gateway, autoscaler, offline) -> Platform:
    return Platform(pool, gateway, autoscaler, offline)
