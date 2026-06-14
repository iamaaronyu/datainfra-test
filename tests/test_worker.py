import threading

from compute_platform.inference import MockEngine
from compute_platform.sharder import Sharder
from compute_platform.worker import Worker


def _enqueue_job(store, config, queue, registry, dataset, model="small-7B",
                 n=200, poison_at=None, target_seconds=1):
    key, _ = dataset(n=n, poison_at=poison_at)
    config.target_shard_seconds = target_seconds
    spec = registry.get(model)
    shards = Sharder(store, config).split("job-w", model, key, spec)
    queue.enqueue_many(shards)
    return len(shards)


def test_worker_processes_all_shards(store, config, queue, registry, dataset):
    nshards = _enqueue_job(store, config, queue, registry, dataset, n=200)
    w = Worker("w1", queue, store, MockEngine("small-7B"), "small-7B", "out/job-w")
    w.run()
    assert w.shards_done == nshards
    assert queue.stats("job-w").finished
    # 输出存在且内容可预测
    outs = store.list("out/job-w")
    assert len(outs) == nshards
    sample = store.read(outs[0]).decode()
    assert sample.startswith("[small-7B] ")


def test_worker_output_is_idempotent(store, config, queue, registry, dataset):
    # 235B: 20 rows/shard -> 2 shards
    _enqueue_job(store, config, queue, registry, dataset, model="Qwen3.5-VL-235B",
                 n=40, target_seconds=1)
    w = Worker("w", queue, store, MockEngine("Qwen3.5-VL-235B"),
               "Qwen3.5-VL-235B", "out/job-w")
    w.run()
    before = {k: store.read(k) for k in store.list("out/job-w")}
    assert before  # 有输出

    # 模拟"数据写了状态没标"：强制把所有分片重置回 pending 再跑一遍。
    # 输出 key 由 shard_id 决定，重复执行即覆盖写，内容必须逐字节一致（无副作用）。
    queue.retry_dead_letters("job-w")  # 无死信，no-op
    import sqlite3
    con = sqlite3.connect(config.db_path)
    con.execute("UPDATE shards SET status='pending', lease_owner=NULL,"
                " lease_expire_at=NULL WHERE job_id='job-w'")
    con.commit()
    con.close()
    w2 = Worker("w2", queue, store, MockEngine("Qwen3.5-VL-235B"),
                "Qwen3.5-VL-235B", "out/job-w")
    w2.run()
    after = {k: store.read(k) for k in store.list("out/job-w")}
    assert after == before  # 重跑覆盖写，逐字节一致


def test_poison_row_goes_to_deadletter(store, config, queue, registry, dataset):
    # 200 行，第 5 行投毒；235B 20 rows/shard -> 第 0 个分片含毒
    _enqueue_job(store, config, queue, registry, dataset, model="Qwen3.5-VL-235B",
                 n=200, poison_at={5}, target_seconds=1)
    w = Worker("w", queue, store, MockEngine("Qwen3.5-VL-235B"),
               "Qwen3.5-VL-235B", "out/job-w", batch_size=8)
    w.run()
    st = queue.stats("job-w")
    assert st.dead == 1          # 毒分片重试超限转死信
    assert st.pending == 0 and st.running == 0
    assert w.shards_done == 9    # 其余 9 个分片成功


def test_worker_graceful_stop(store, config, queue, registry, dataset):
    _enqueue_job(store, config, queue, registry, dataset, model="Qwen3.5-VL-235B",
                 n=200, target_seconds=1)  # 10 shards
    stop = threading.Event()

    class StoppingEngine(MockEngine):
        def __init__(self, outer, *a):
            super().__init__(*a)
            self.outer = outer

        def generate(self, batch):
            self.outer["count"] += 1
            if self.outer["count"] >= 2:
                stop.set()  # 处理两批后请求停止
            return super().generate(batch)

    ctx = {"count": 0}
    w = Worker("w", queue, store, StoppingEngine(ctx, "Qwen3.5-VL-235B"),
               "Qwen3.5-VL-235B", "out/job-w", batch_size=20)
    w.run(stop_event=stop)
    st = queue.stats("job-w")
    # 收到 stop 后停止领新分片；未完成分片仍在 pending（或个别 running）
    assert st.done < 10
    assert st.pending + st.running > 0
