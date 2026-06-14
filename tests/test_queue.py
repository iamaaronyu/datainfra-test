from compute_platform.models import Shard, ShardStatus
from compute_platform.queue import SqliteShardQueue


def _shards(job="job-1", model="small-7B", n=3):
    return [Shard(shard_id=f"{job}-{i:06d}", job_id=job, model=model,
                  file="in.jsonl", start_offset=i * 10, end_offset=(i + 1) * 10, rows=5)
            for i in range(n)]


def test_enqueue_idempotent(queue: SqliteShardQueue):
    s = _shards(n=3)
    assert queue.enqueue_many(s) == 3
    # 重复入队（切分可重入）应去重
    assert queue.enqueue_many(s) == 0
    assert queue.stats("job-1").pending == 3


def test_claim_and_commit(queue: SqliteShardQueue):
    queue.enqueue_many(_shards(n=2))
    shard = queue.claim("small-7B", 120)
    assert shard is not None and shard.status == ShardStatus.RUNNING
    assert queue.stats("job-1").running == 1
    assert queue.commit(shard.shard_id, shard.lease_owner) is True
    assert queue.stats("job-1").done == 1


def test_claim_returns_none_when_empty(queue: SqliteShardQueue):
    assert queue.claim("small-7B", 120) is None


def test_commit_cas_rejects_wrong_token(queue: SqliteShardQueue):
    queue.enqueue_many(_shards(n=1))
    shard = queue.claim("small-7B", 120)
    assert queue.commit(shard.shard_id, "not-the-owner") is False
    assert queue.stats("job-1").running == 1  # 仍在执行中


def test_renew_extends_lease(queue: SqliteShardQueue):
    t = [1000.0]
    queue.clock = lambda: t[0]
    queue.enqueue_many(_shards(n=1))
    shard = queue.claim("small-7B", 100)   # 到期 1100
    t[0] = 1090.0
    assert queue.renew(shard.shard_id, shard.lease_owner, 100) is True  # 续到 1190
    t[0] = 1150.0
    # 还没过期，reap 不应回收
    assert queue.reap_expired() == 0
    assert queue.stats("job-1").running == 1


def test_lease_expiry_reclaims(queue: SqliteShardQueue):
    t = [1000.0]
    queue.clock = lambda: t[0]
    queue.enqueue_many(_shards(n=1))
    shard = queue.claim("small-7B", 100)   # 到期 1100
    t[0] = 1200.0                          # 已过期
    assert queue.reap_expired() == 1
    st = queue.stats("job-1")
    assert st.pending == 1 and st.running == 0
    # 另一个 worker 能重新领到
    again = queue.claim("small-7B", 100)
    assert again is not None and again.retries == 1


def test_claim_auto_reaps_expired(queue: SqliteShardQueue):
    t = [1000.0]
    queue.clock = lambda: t[0]
    queue.enqueue_many(_shards(n=1))
    s1 = queue.claim("small-7B", 100)
    t[0] = 2000.0
    s2 = queue.claim("small-7B", 100)      # claim 内部先 reap
    assert s2 is not None
    assert s2.lease_owner != s1.lease_owner


def test_fail_retries_then_deadletters(queue: SqliteShardQueue):
    queue.enqueue_many(_shards(n=1))
    sid = "job-1-000000"
    # max_retries=3：第 4 次失败转死信
    for _ in range(3):
        shard = queue.claim("small-7B", 120)
        assert queue.fail(shard.shard_id, shard.lease_owner) is True
        assert queue.stats("job-1").pending == 1
    shard = queue.claim("small-7B", 120)
    queue.fail(shard.shard_id, shard.lease_owner)
    st = queue.stats("job-1")
    assert st.dead == 1 and st.pending == 0
    assert [s.shard_id for s in queue.dead_letters("job-1")] == [sid]


def test_abort_releases_without_retry(queue: SqliteShardQueue):
    queue.enqueue_many(_shards(n=1))
    shard = queue.claim("small-7B", 120)
    assert queue.abort(shard.shard_id, shard.lease_owner) is True
    again = queue.claim("small-7B", 120)
    assert again.retries == 0  # abort 不计重试（被抢占语义）


def test_cancel_job(queue: SqliteShardQueue):
    queue.enqueue_many(_shards(n=3))
    queue.claim("small-7B", 120)
    assert queue.cancel_job("job-1") == 3
    st = queue.stats("job-1")
    assert st.cancelled == 3 and st.pending == 0 and st.running == 0
    assert queue.claim("small-7B", 120) is None


def test_retry_dead_letters(queue: SqliteShardQueue):
    queue.enqueue_many(_shards(n=1))
    for _ in range(4):
        shard = queue.claim("small-7B", 120)
        queue.fail(shard.shard_id, shard.lease_owner)
    assert queue.stats("job-1").dead == 1
    assert queue.retry_dead_letters("job-1") == 1
    st = queue.stats("job-1")
    assert st.dead == 0 and st.pending == 1


def test_model_isolation_in_claim(queue: SqliteShardQueue):
    queue.enqueue_many(_shards(job="job-1", model="small-7B", n=1))
    queue.enqueue_many(_shards(job="job-2", model="Qwen3.5-VL-235B", n=1))
    # 领 small-7B 不应拿到 235B 的分片（同模型聚批）
    shard = queue.claim("small-7B", 120)
    assert shard.model == "small-7B"
    assert queue.claim("DeepSeek-V4", 120) is None


def test_finished_semantics(queue: SqliteShardQueue):
    queue.enqueue_many(_shards(n=2))
    assert not queue.stats("job-1").finished
    for _ in range(2):
        s = queue.claim("small-7B", 120)
        queue.commit(s.shard_id, s.lease_owner)
    assert queue.stats("job-1").finished
