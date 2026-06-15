"""NPU 统一推理算力池 — 参考实现。

对应《docs/NPU统一推理算力池需求说明_v2》。把"两类业务抢同一池卡"的旧 HPC
模型，重构为"统一多模型推理算力池"：GLM5.1 单部署在线/离线优先级混跑、在线副本
按负载在 64–3000 卡间自动扩缩、其余卡按需装载多模型跑幂等分片离线任务。
"""
from .autoscaler import Autoscaler, AutoscalerConfig, PrewarmWindow
from .cache_affinity import CacheAffinityRouter
from .card_pool import CardPool, ScalePlan
from .gateway import InferenceGateway, StepResult
from .models import (
    Card,
    CardRole,
    CardType,
    LoadMetrics,
    ModelSpec,
    ONLINE_MODEL,
    Priority,
    Request,
    ShardTask,
)
from .offline_pool import OfflinePool, make_shards
from .platform import Platform

__all__ = [
    "Autoscaler",
    "AutoscalerConfig",
    "PrewarmWindow",
    "CacheAffinityRouter",
    "CardPool",
    "ScalePlan",
    "InferenceGateway",
    "StepResult",
    "Card",
    "CardRole",
    "CardType",
    "LoadMetrics",
    "ModelSpec",
    "ONLINE_MODEL",
    "Priority",
    "Request",
    "ShardTask",
    "OfflinePool",
    "make_shards",
    "Platform",
]
