import os
import pytest

from compute_platform.config import Config
from compute_platform.objectstore import LocalObjectStore
from compute_platform.queue import SqliteShardQueue
from compute_platform.registry import ModelRegistry
from compute_platform.batch_api.service import JobStore


@pytest.fixture
def config(tmp_path) -> Config:
    return Config(
        store_root=str(tmp_path / "store"),
        db_path=str(tmp_path / "state.db"),
        lease_seconds=120,
        max_retries=3,
        target_shard_seconds=10,   # 小粒度便于测试多分片
    )


@pytest.fixture
def store(config) -> LocalObjectStore:
    return LocalObjectStore(config.store_root)


@pytest.fixture
def queue(config) -> SqliteShardQueue:
    return SqliteShardQueue(config.db_path, max_retries=config.max_retries)


@pytest.fixture
def job_store(config) -> JobStore:
    return JobStore(config.db_path)


@pytest.fixture
def registry() -> ModelRegistry:
    return ModelRegistry.with_defaults()


@pytest.fixture
def dataset(store):
    """写一个 jsonl 输入数据集，返回 (key, 行数)。"""
    def _make(key="datasets/input.jsonl", n=200, poison_at=None):
        lines = []
        for i in range(n):
            if poison_at is not None and i in poison_at:
                lines.append(f'{{"id": {i}, "text": "POISON row {i}"}}')
            else:
                lines.append(f'{{"id": {i}, "text": "row {i}"}}')
        store.write(key, ("\n".join(lines) + "\n").encode("utf-8"))
        return key, n
    return _make
