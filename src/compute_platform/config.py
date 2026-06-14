"""平台配置（环境变量前缀 CP_）。

对应设计文档 §12 的待定参数：租约时长、分片目标时长、加载成本门槛 K、
安全水位、重试上限等都集中在此，便于试点调参。
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


@dataclass
class Config:
    # 存储面
    store_root: str = os.getenv("CP_STORE_ROOT", "/tmp/cp-store")
    db_path: str = os.getenv("CP_DB_PATH", "/tmp/cp-state.db")

    # 分片队列 / 租约（§6.3）
    lease_seconds: int = _int("CP_LEASE_SECONDS", 120)
    max_retries: int = _int("CP_MAX_RETRIES", 3)          # 超限转死信
    max_shards_per_job: int = _int("CP_MAX_SHARDS", 1_000_000)

    # 切分粒度（§6.2）：单分片目标运行时长（秒），由模型吞吐反推条数
    target_shard_seconds: int = _int("CP_TARGET_SHARD_SECONDS", 120)

    # 弹性控制器（§6.4）
    safety_headroom: float = _float("CP_SAFETY_HEADROOM", 0.05)  # 供给侧保留水位
    load_cost_k: int = _int("CP_LOAD_COST_K", 5)                 # 加载成本门槛倍数

    # 三池模型（架构 §4.3，决策三）
    total_cards: int = _int("CP_TOTAL_CARDS", 5000)              # 试点机房总卡（贵阳 ~5000）
    online_reserved_cards: int = _int("CP_ONLINE_RESERVED", 3000)  # 在线保障池（含排空余量）
    # 抢占排空余量：在线拿卡要等离线在 batch 边界优雅退出，这段时延须计入在线保障池
    # （review 🟢5）。在线保障池 = 在线峰值 + ceil(峰值斜率 × 排空时延)。
    preempt_drain_seconds: int = _int("CP_PREEMPT_DRAIN_SECONDS", 120)  # SIGTERM grace

    # 计量结算（§8）—— 价格杠杆
    base_price_per_card_hour: float = _float("CP_BASE_PRICE", 10.0)
    b3_price_factor: float = _float("CP_B3_FACTOR", 1.5)   # B3 更强，单价更高
    b4_price_factor: float = _float("CP_B4_FACTOR", 1.0)
    preemptible_discount: float = _float("CP_PREEMPT_DISCOUNT", 0.4)  # 可抢占 4 折
