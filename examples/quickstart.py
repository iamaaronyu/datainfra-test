"""零外部依赖的端到端 demo：进程内拉起离线批处理全链路。

演示设计 §6 的闭环：提交作业 → 切分入队 → 多 worker 并发拉分片本地推理 →
幂等写回 → 进度 100%；并演示一次抢占后续跑(分片回队列不丢)与差异计量。

运行：python examples/quickstart.py
"""
import sys
import tempfile
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from compute_platform.config import Config
from compute_platform.objectstore import LocalObjectStore
from compute_platform.queue import SqliteShardQueue
from compute_platform.registry import ModelRegistry
from compute_platform.batch_api.service import JobStore, BatchService, SubmitRequest
from compute_platform.governance import QuotaManager, MeteringService
from compute_platform.governance.metering import UsageEvent
from compute_platform.models import CardType, QoS
from compute_platform.inference import MockEngine
from compute_platform.worker import Worker


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="cp-demo-")
    cfg = Config(store_root=f"{tmp}/store", db_path=f"{tmp}/state.db",
                 target_shard_seconds=1)
    store = LocalObjectStore(cfg.store_root)
    queue = SqliteShardQueue(cfg.db_path, max_retries=cfg.max_retries)
    registry = ModelRegistry.with_defaults()
    quota = QuotaManager({"data-team": 64})
    svc = BatchService(cfg, store, queue, registry, JobStore(cfg.db_path), quota)

    # 1) 业务方把数据集放进对象存储（亿级条目此处缩成 1000 行）
    n = 1000
    rows = "\n".join(f'{{"id": {i}, "text": "raw doc {i}"}}' for i in range(n))
    store.write("datasets/pretrain.jsonl", (rows + "\n").encode())

    # 2) 提交批作业（只传数据集 + 模型名）
    job = svc.submit("data-team", SubmitRequest(
        model="Qwen3.5-VL-235B", input_key="datasets/pretrain.jsonl",
        output_prefix="out/clean", prompt_template="clean: {text}"))
    print(f"提交作业 {job.job_id}：切成 {job.total_shards} 个分片（20 行/片）")

    # 3) 弹性 worker 池：4 个 worker 并发填谷
    workers = [Worker(f"w{i}", queue, store, MockEngine("Qwen3.5-VL-235B"),
                      "Qwen3.5-VL-235B", "out/clean") for i in range(4)]
    threads = [threading.Thread(target=w.run) for w in workers]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    prog = svc.progress(job.job_id)
    each = ", ".join(f"{w.worker_id}={w.shards_done}" for w in workers)
    print(f"4 worker 并发完成：{each}（合计 {sum(w.shards_done for w in workers)} 分片）")
    print(f"作业状态：{prog['status']}，进度 {prog['progress']*100:.0f}%")

    out_rows = sum(len([x for x in store.read(k).decode().split('\n') if x])
                   for k in store.list("out/clean"))
    print(f"输出行数 {out_rows} == 输入行数 {n}：{'✓ 无重复无丢失' if out_rows == n else '✗'}")

    # 4) 差异计量：在线 Guaranteed 全价 vs 离线 Preemptible 折价（被抢占时段不计）
    m = MeteringService(cfg)
    online = m.cost_of(UsageEvent("vibecode", CardType.B3, QoS.GUARANTEED, 8, 3600))
    offline = m.cost_of(UsageEvent("data-team", CardType.B3, QoS.PREEMPTIBLE, 8, 3600))
    offline_preempted = m.cost_of(
        UsageEvent("data-team", CardType.B3, QoS.PREEMPTIBLE, 8, 3600, 1800))
    print(f"\n计量（8×B3 跑 1 小时）：")
    print(f"  在线 Guaranteed 全价      = {online:.0f}")
    print(f"  离线 Preemptible 4 折     = {offline:.0f}")
    print(f"  离线被抢占一半（半价计费）= {offline_preempted:.0f}")

    print(f"\n临时数据目录：{tmp}")


if __name__ == "__main__":
    main()
