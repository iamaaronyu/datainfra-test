"""统一推理算力池 — 领域模型（对应《NPU统一推理算力池需求说明_v2》）。

核心转变：不再是"业务 QoS 配额 + 抢占"，而是"卡角色（在线副本/温卡/离线）
+ 模型部署"。同一张卡在不同时刻可以是在线副本、温卡（跑 GLM5.1 批量，
免换装秒级转在线）、或离线池中装载了某个模型的卡。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

#: GLM5.1 是横跨在线与离线的同一模型（§1.2）。
ONLINE_MODEL = "GLM5.1"


class CardType(str, Enum):
    """昇腾卡型（§6.5）。B3 显存/带宽更强，B4 较弱、可承载可降级任务。"""

    B3 = "B3"
    B4 = "B4"


class CardRole(str, Enum):
    """卡在某一时刻的角色（§2 三层架构 / §4.1 资源模型）。"""

    ONLINE = "online"          # GLM5.1 在线副本
    WARM_BATCH = "warm_batch"  # 正在跑 GLM5.1 批量的"温卡"，可秒级转在线（R2.2）
    OFFLINE = "offline"        # 离线池，装载了某个（非 GLM5.1）模型
    LOADING = "loading"        # 换装中（卸载旧模型 + 加载新模型，分钟级，R3.2）
    DRAINING = "draining"      # 正在 drain：不再派发新分片，等在途分片完成（R3.2）


class Priority(str, Enum):
    """推理请求优先级（§6.1 R1.2）。在线请求实时插队，批量请求填谷。"""

    ONLINE = "online"
    BATCH = "batch"


@dataclass
class ModelSpec:
    """模型注册表条目（§6.5 R5.1）。"""

    name: str
    required_card_type: CardType | None = None  # 强依赖卡型（None = 不限）
    degradable: bool = True                      # 是否可降级到非首选卡型
    cards_per_replica: int = 1
    load_seconds: float = 0.0                    # "换装"耗时（R3.2）
    throughput_rows_per_sec: float = 0.0         # 离线吞吐估算


@dataclass
class Card:
    """卡池中的一张卡。"""

    card_id: str
    card_type: CardType
    role: CardRole = CardRole.OFFLINE
    model: str | None = None  # role 为 OFFLINE/WARM_BATCH/ONLINE 时，当前装载的模型


@dataclass
class Request:
    """一次推理请求（§6.1）。"""

    request_id: str
    priority: Priority
    model: str = ONLINE_MODEL
    session_key: str | None = None  # Claude Code 会话/repo 前缀（§6.4 缓存亲和）
    tokens: int = 1


@dataclass
class LoadMetrics:
    """在线侧实时负载（§6.2 R2.1，§6.6 R6.1）。"""

    qps: float = 0.0
    queue_depth: int = 0
    ttft_p95: float = 0.0


@dataclass
class ShardTask:
    """离线分片任务声明（§6.3 R3.5）。声明模型/卡型/卡数/分片数，无需声明 checkpoint 能力。"""

    job_id: str
    model: str
    cards: int
    total_shards: int
    deadline: float | None = None  # 可选截止时间（§5.8）
