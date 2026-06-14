"""数据局部性硬约束（架构 §5.1，需求 §8.4）。

几 PB 不可跨域搬：调度器从 DataHub 读 dataset 的 region 分布，**强制把 Ray 作业调到
数据所在域**（每地域一个 Ray 集群）。跨域只允许显式放行的小流量，且须错峰。
"""
from __future__ import annotations

from dataclasses import dataclass


class LocalityViolation(Exception):
    """作业被要求跨域处理 PB 级数据，违反局部性硬约束。"""


@dataclass(frozen=True)
class Placement:
    region: str
    cross_region: bool
    reason: str


def resolve_placement(
    dataset_region: str,
    ray_clusters: dict[str, bool],
    *,
    allow_cross_region: bool = False,
) -> Placement:
    """决定作业落在哪个地域的 Ray 集群。

    ray_clusters = {region: 是否可用}。默认强制就近（数据所在域）；该域不可用时，
    除非 allow_cross_region 显式放行，否则抛 LocalityViolation（宁可排队等本域恢复，
    也不把 PB 数据拖过专线）。
    """
    if not dataset_region:
        raise LocalityViolation("dataset 缺少 region 元数据（DataHub 未登记物理分布）")
    if ray_clusters.get(dataset_region):
        return Placement(dataset_region, cross_region=False, reason="与数据同域，就近处理")
    if not allow_cross_region:
        raise LocalityViolation(
            f"数据所在域 {dataset_region} 无可用 Ray 集群，且未放行跨域；"
            f"PB 数据不跨专线——应等本域恢复或扩容")
    fallback = next((r for r, ok in ray_clusters.items() if ok), None)
    if fallback is None:
        raise LocalityViolation("无任何可用 Ray 集群")
    return Placement(fallback, cross_region=True, reason=f"本域不可用，已显式放行跨域至 {fallback}")
