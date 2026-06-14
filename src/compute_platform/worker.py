"""离线 worker（执行面，§6.3）。

主循环：claim → 读分片字节 → 本地 generate → 续租 → 原子写回 → commit。
铁律：无状态、随时可杀。被抢占时在 batch 边界放弃当前分片（租约自然过期回队列）。
确定性失败（毒分片）调 fail() 累计重试 → 死信。
"""
from __future__ import annotations

import threading

from .inference import InferenceEngine
from .objectstore import LocalObjectStore
from .queue.base import ShardQueue


class Worker:
    def __init__(
        self,
        worker_id: str,
        queue: ShardQueue,
        store: LocalObjectStore,
        engine: InferenceEngine,
        model: str,
        output_prefix: str,
        lease_seconds: int = 120,
        batch_size: int = 16,
    ):
        self.worker_id = worker_id
        self.queue = queue
        self.store = store
        self.engine = engine
        self.model = model
        self.output_prefix = output_prefix
        self.lease_seconds = lease_seconds
        self.batch_size = batch_size
        self.shards_done = 0
        self.shards_failed = 0
        self.shards_abandoned = 0

    def _read_rows(self, shard) -> list[str]:
        raw = self.store.read_range(shard.file, shard.start_offset, shard.end_offset)
        text = raw.decode("utf-8")
        return [ln for ln in text.split("\n") if ln != ""]

    def run_once(self) -> bool:
        """领一个分片跑完。返回 False 表示没活可领（应退出）。"""
        shard = self.queue.claim(self.model, self.lease_seconds)
        if shard is None:
            return False
        token = shard.lease_owner
        rows = self._read_rows(shard)
        results: list[str] = []
        try:
            for i in range(0, len(rows), self.batch_size):
                batch = rows[i:i + self.batch_size]
                results.extend(self.engine.generate(batch))
                self.queue.renew(shard.shard_id, token, self.lease_seconds)
        except Exception:
            # 确定性失败 → 累计重试，超限转死信（毒分片）
            self.queue.fail(shard.shard_id, token)
            self.shards_failed += 1
            return True
        # 幂等提交：输出 key 由 shard_id 决定、原子写、commit 最后一步
        out_key = f"{self.output_prefix}/{shard.shard_id}.done"
        self.store.write(out_key, ("\n".join(results) + "\n").encode("utf-8"))
        self.queue.commit(shard.shard_id, token)
        self.shards_done += 1
        return True

    def run(self, stop_event: threading.Event | None = None) -> None:
        """领活循环：拿不到分片就退出释放卡（§6.4 缩容靠饿死）。

        收到 stop（被抢占）则不再领新分片；当前分片在 run_once 内已完成或失败。
        """
        self.engine.load()
        while True:
            if stop_event is not None and stop_event.is_set():
                break
            if not self.run_once():
                break
