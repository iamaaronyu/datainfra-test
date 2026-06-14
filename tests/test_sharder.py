from compute_platform.sharder import Sharder


def test_pointer_shards_cover_all_rows(store, config, registry, dataset):
    key, n = dataset(n=200)
    spec = registry.get("small-7B")  # throughput 120, target 10s -> 1200 rows/shard
    sharder = Sharder(store, config)
    shards = sharder.split("job-x", "small-7B", key, spec)
    assert sum(s.rows for s in shards) == n


def test_granularity_from_throughput(store, config, registry, dataset):
    key, n = dataset(n=200)
    # 235B throughput 20, target 10s -> 200 rows/shard -> 1 shard
    spec = registry.get("Qwen3.5-VL-235B")
    sharder = Sharder(store, config)
    shards = sharder.split("job-x", "Qwen3.5-VL-235B", key, spec)
    assert sharder.rows_per_shard(spec) == 200
    assert len(shards) == 1


def test_multiple_shards_when_small_granularity(store, config, registry, dataset):
    key, n = dataset(n=200)
    spec = registry.get("Qwen3.5-VL-235B")
    config.target_shard_seconds = 1   # 20 rows/shard -> 10 shards
    sharder = Sharder(store, config)
    shards = sharder.split("job-x", "Qwen3.5-VL-235B", key, spec)
    assert len(shards) == 10
    assert all(s.rows == 20 for s in shards)


def test_pointer_ranges_are_correct(store, config, registry, dataset):
    key, n = dataset(n=50)
    spec = registry.get("Qwen3.5-VL-235B")
    config.target_shard_seconds = 1   # 20 rows/shard
    sharder = Sharder(store, config)
    shards = sharder.split("job-x", "Qwen3.5-VL-235B", key, spec)
    # 用 range read 读回，行数应吻合
    for s in shards:
        raw = store.read_range(s.file, s.start_offset, s.end_offset)
        rows = [ln for ln in raw.decode().split("\n") if ln]
        assert len(rows) == s.rows


def test_deterministic_shard_ids(store, config, registry, dataset):
    key, n = dataset(n=200)
    spec = registry.get("small-7B")
    sharder = Sharder(store, config)
    a = sharder.split("job-x", "small-7B", key, spec)
    b = sharder.split("job-x", "small-7B", key, spec)
    assert [s.shard_id for s in a] == [s.shard_id for s in b]
