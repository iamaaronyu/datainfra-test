"""端到端：把设计 §6 的核心闭环串起来跑。

覆盖场景 S2(离线作业执行)、S4(被抢占续跑)、S5(多 worker 填谷并发)。
验证"状态外置、worker 无状态可杀 → 容错/抢占同一码路"的设计基石。
"""
import threading

from compute_platform.batch_api import BatchService
from compute_platform.batch_api.service import SubmitRequest
from compute_platform.governance import QuotaManager
from compute_platform.inference import MockEngine
from compute_platform.models import QoS
from compute_platform.worker import Worker


def _service(config, store, queue, registry, job_store):
    config.target_shard_seconds = 1
    quota = QuotaManager({"team-a": 256})
    return BatchService(config, store, queue, registry, job_store, quota)


def test_full_pipeline_single_worker(config, store, queue, registry, job_store, dataset):
    dataset(key="ds.jsonl", n=500)
    svc = _service(config, store, queue, registry, job_store)
    job = svc.submit("team-a", SubmitRequest(
        model="Qwen3.5-VL-235B", input_key="ds.jsonl", output_prefix="out/e2e"))
    # 235B 20 rows/shard -> 25 shards
    assert job.total_shards == 25

    w = Worker("w", queue, store, MockEngine("Qwen3.5-VL-235B"),
               "Qwen3.5-VL-235B", "out/e2e")
    w.run()

    prog = svc.progress(job.job_id)
    assert prog["status"] == "succeeded"
    assert prog["done"] == 25
    # 输出总行数 == 输入行数
    total_out = 0
    for k in store.list("out/e2e"):
        total_out += len([ln for ln in store.read(k).decode().split("\n") if ln])
    assert total_out == 500


def test_preemption_then_resume(config, store, queue, registry, job_store, dataset):
    """worker 跑到一半被抢占(kill)，分片回队列，新 worker 续跑到 100%。"""
    dataset(key="ds.jsonl", n=400)
    svc = _service(config, store, queue, registry, job_store)
    job = svc.submit("team-a", SubmitRequest(
        model="Qwen3.5-VL-235B", input_key="ds.jsonl", output_prefix="out/preempt"))
    nshards = job.total_shards  # 20

    # worker A 处理几个分片后被"抢占"：置 stop，循环在分片边界停
    stop = threading.Event()
    a = Worker("A", queue, store, MockEngine("Qwen3.5-VL-235B"),
               "Qwen3.5-VL-235B", "out/preempt")
    a.engine.load()
    for _ in range(5):
        a.run_once()
    done_by_a = a.shards_done
    assert 0 < done_by_a < nshards

    # 模拟"硬抢占"：还有一个分片正被领着没提交（手动制造一个 orphan）
    orphan = queue.claim("Qwen3.5-VL-235B", 100)
    assert orphan is not None  # A 之外又领走一个，但不提交（worker 被 kill）

    # 租约过期回收（时间快进）—— 等价于 kill -9 后的自愈
    t = [queue.clock() + 10_000]
    queue.clock = lambda: t[0]
    reaped = queue.reap_expired()
    assert reaped >= 1  # orphan 回到 pending

    # worker B 接力跑完
    b = Worker("B", queue, store, MockEngine("Qwen3.5-VL-235B"),
               "Qwen3.5-VL-235B", "out/preempt")
    b.run()

    prog = svc.progress(job.job_id)
    assert prog["status"] == "succeeded"
    assert prog["done"] == nshards          # 不丢分片
    assert prog["progress"] == 1.0
    # 幂等：输出行数恰好等于输入（无重复、无丢失）
    total_out = sum(
        len([ln for ln in store.read(k).decode().split("\n") if ln])
        for k in store.list("out/preempt"))
    assert total_out == 400


def test_concurrent_worker_pool(config, store, queue, registry, job_store, dataset):
    """多 worker 并发填谷，无重复无丢失（租约 claim 原子性）。"""
    dataset(key="ds.jsonl", n=600)
    svc = _service(config, store, queue, registry, job_store)
    job = svc.submit("team-a", SubmitRequest(
        model="Qwen3.5-VL-235B", input_key="ds.jsonl", output_prefix="out/pool"))
    nshards = job.total_shards  # 30

    workers = [Worker(f"w{i}", queue, store, MockEngine("Qwen3.5-VL-235B"),
                      "Qwen3.5-VL-235B", "out/pool") for i in range(4)]
    threads = [threading.Thread(target=w.run) for w in workers]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    prog = svc.progress(job.job_id)
    assert prog["status"] == "succeeded"
    assert prog["done"] == nshards
    # 所有 worker 完成数之和 == 总分片数（无重复领取）
    assert sum(w.shards_done for w in workers) == nshards
    # 每个分片恰好一个输出文件
    assert len(store.list("out/pool")) == nshards
    total_out = sum(
        len([ln for ln in store.read(k).decode().split("\n") if ln])
        for k in store.list("out/pool"))
    assert total_out == 600
