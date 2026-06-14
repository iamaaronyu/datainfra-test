"""切分器（§6.2）。

三条核心：
1. 切指针不切数据 —— 分片只是 {file, start_offset, end_offset, rows} 元数据，
   worker 用 range read 读自己那段。
2. 粒度由目标时长反推 —— 分片条数 = 目标时长 × 模型估算吞吐。
3. 切分可重入 —— 确定性 shard_id，配合 enqueue 的 INSERT OR IGNORE 去重。
"""
from __future__ import annotations

from .config import Config
from .models import ModelSpec, Shard, ShardStatus
from .objectstore import LocalObjectStore


class Sharder:
    def __init__(self, store: LocalObjectStore, config: Config):
        self.store = store
        self.config = config

    def rows_per_shard(self, spec: ModelSpec) -> int:
        n = int(self.config.target_shard_seconds * spec.throughput_rows_per_sec)
        return max(1, n)

    def split(self, job_id: str, model: str, input_key: str, spec: ModelSpec) -> list[Shard]:
        """扫描 jsonl 换行偏移，按 rows_per_shard 分组成指针分片。

        确定性：同 (job_id, input_key) 永远切出同样的 shard_id 集合。
        """
        data = self.store.read(input_key)
        # 记录每行的 [start, end) 字节区间（end 含换行符）
        line_spans: list[tuple[int, int]] = []
        start = 0
        for i, b in enumerate(data):
            if b == 0x0A:  # '\n'
                line_spans.append((start, i + 1))
                start = i + 1
        if start < len(data):  # 末行无换行
            line_spans.append((start, len(data)))

        per = self.rows_per_shard(spec)
        shards: list[Shard] = []
        idx = 0
        for g in range(0, len(line_spans), per):
            group = line_spans[g:g + per]
            byte_start = group[0][0]
            byte_end = group[-1][1]
            shards.append(Shard(
                shard_id=f"{job_id}-{idx:06d}",
                job_id=job_id,
                model=model,
                file=input_key,
                start_offset=byte_start,
                end_offset=byte_end,
                rows=len(group),
                status=ShardStatus.PENDING,
            ))
            idx += 1
            if idx > self.config.max_shards_per_job:
                raise ValueError("shard count exceeds CP_MAX_SHARDS guard")
        return shards
