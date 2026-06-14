"""算力服务化平台 — 在线/离线混部调度核心。

对应《算力服务化平台总体设计v1.0》的可运行、可全量测试实现。
真实环境里的 vLLM-Ascend / 对象存储 / Redis-PG / Volcano 在此分别被
MockEngine / LocalObjectStore / SqliteShardQueue / 纯函数控制器替身，
以便不依赖昇腾硬件即可验证全部机制（租约队列、幂等提交、弹性伸缩、
QoS 抢占、差异计量）。
"""

from .config import Config
from .models import QoS, CardType, JobStatus, ShardStatus, ModelSpec, Shard, Job

__all__ = [
    "Config",
    "QoS",
    "CardType",
    "JobStatus",
    "ShardStatus",
    "ModelSpec",
    "Shard",
    "Job",
]
