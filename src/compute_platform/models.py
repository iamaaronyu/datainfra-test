"""领域模型与枚举。

核心抽象（设计 §10.2）：QoS 等级、卡型、作业 → 分片、模型规格。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class QoS(str, Enum):
    """服务质量分级（§7.1）。决定调度优先级与计费倍率。"""

    GUARANTEED = "guaranteed"          # 在线：独占、不可抢、全价
    PREEMPTIBLE = "preemptible"        # 离线可抢：填空闲、随时让路、折价
    BEST_EFFORT_FIXED = "best_effort"  # 离线不改造：固定低配额、不进混部、全价


class CardType(str, Enum):
    """昇腾 A2 异构卡型（§9）。"""

    B3 = "B3"  # 64G 显存、算力强 —— 235B / 在线主力
    B4 = "B4"  # 32G 显存、算力弱 —— 小模型 / 小任务


class JobStatus(str, Enum):
    """作业状态机（§6.2）。含"部分成功"态。"""

    VALIDATING = "validating"
    SPLITTING = "splitting"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ShardStatus(str, Enum):
    """分片生命周期（§6.3）。"""

    PENDING = "pending"      # 待领取
    RUNNING = "running"      # 执行中（持租约）
    DONE = "done"            # 已提交
    DEAD = "dead"            # 死信（重试超限）
    CANCELLED = "cancelled"  # 随作业取消


@dataclass
class ModelSpec:
    """模型注册表条目（§4.3）。供切分器定粒度、控制器定加载门槛。"""

    name: str
    card_type: CardType
    cards_per_worker: int          # 一个 worker 实例占几张卡（235B=8, 16卡组等）
    load_seconds: float            # 模型加载常驻显存的耗时（235B 十几分钟）
    throughput_rows_per_sec: float  # 离线模式单 worker 估算吞吐


@dataclass
class Shard:
    """分片 = 指针元数据，不复制数据（§6.2 切指针不切数据）。"""

    shard_id: str
    job_id: str
    model: str
    file: str          # 对象存储中的输入 key
    start_offset: int  # 字节起始
    end_offset: int    # 字节结束（不含）
    rows: int
    status: ShardStatus = ShardStatus.PENDING
    retries: int = 0
    lease_owner: str | None = None
    lease_expire_at: float | None = None


@dataclass
class Job:
    """离线批作业。"""

    job_id: str
    tenant: str
    model: str
    input_key: str
    output_prefix: str
    template_hash: str
    status: JobStatus = JobStatus.VALIDATING
    total_shards: int = 0
    client_token: str | None = None
    created_at: float = 0.0
    qos: QoS = QoS.PREEMPTIBLE
