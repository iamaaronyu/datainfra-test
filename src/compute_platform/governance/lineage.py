"""数据集版本链与血缘登记 —— DataHub 替身（架构 §5.3，需求 §5）。

真实环境是 DataHub；此处用进程内 registry 做可测试替身，承载 v2.0 的两件事：
1. **多轮 = 版本链**：每轮一次作业产出一个不可变新版本，round N 输出 = round N+1 输入；
2. **task × DataHub 闭环**：提交读输入版本、完成回写一条血缘边
   （输入版本 →[作业:模型+模板+参数]→ 输出版本），并登记 region 供局部性调度。

端到端血缘 = 沿 parents 回溯，可回答「从哪来、经哪些加工、用哪版逻辑」，并支持复现校验。
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field


class LineageError(Exception):
    pass


@dataclass(frozen=True)
class DatasetVersion:
    version_id: str
    dataset: str
    round: int
    region: str                      # 物理分布，供数据局部性调度
    parents: tuple[str, ...]         # 上游版本（多轮/多输入）
    job_id: str
    model: str
    template_hash: str
    params_hash: str
    created_at: float = field(default_factory=time.time)


class LineageRegistry:
    """DataHub 的可测试替身：登记不可变版本 + 端到端血缘 + region。"""

    def __init__(self) -> None:
        self._versions: dict[str, DatasetVersion] = {}
        self._lock = threading.Lock()

    def register(
        self,
        *,
        dataset: str,
        round: int,
        region: str,
        job_id: str,
        model: str,
        template_hash: str,
        params_hash: str,
        parents: tuple[str, ...] = (),
    ) -> DatasetVersion:
        """登记一个新版本（完成时由 task 回写）。校验 parents 存在，保证血缘不断裂。"""
        with self._lock:
            for p in parents:
                if p not in self._versions:
                    raise LineageError(f"父版本不存在: {p}（血缘会断裂）")
            vid = f"{dataset}@r{round}-{uuid.uuid4().hex[:8]}"
            v = DatasetVersion(
                version_id=vid, dataset=dataset, round=round, region=region,
                parents=tuple(parents), job_id=job_id, model=model,
                template_hash=template_hash, params_hash=params_hash)
            self._versions[vid] = v
            return v

    def get(self, version_id: str) -> DatasetVersion:
        with self._lock:
            v = self._versions.get(version_id)
        if v is None:
            raise LineageError(f"版本不存在: {version_id}")
        return v

    def region_of(self, version_id: str) -> str:
        """供 locality.resolve_placement 读 dataset region。"""
        return self.get(version_id).region

    def latest(self, dataset: str) -> DatasetVersion | None:
        """某数据集 round 最大、created_at 最新的版本（驱动下一轮输入）。"""
        with self._lock:
            cands = [v for v in self._versions.values() if v.dataset == dataset]
        if not cands:
            return None
        return max(cands, key=lambda v: (v.round, v.created_at))

    def ancestors(self, version_id: str) -> list[DatasetVersion]:
        """端到端血缘：沿 parents 回溯的全部祖先版本（去重，含跨轮跨产线）。"""
        seen: dict[str, DatasetVersion] = {}
        stack = list(self.get(version_id).parents)
        while stack:
            vid = stack.pop()
            if vid in seen:
                continue
            v = self.get(vid)
            seen[vid] = v
            stack.extend(v.parents)
        return list(seen.values())

    def reproducible_with(self, version_id: str, template_hash: str, params_hash: str) -> bool:
        """复现校验：同输入 + 同模板 hash + 同参数 → 应产出等价结果（需求 §5 可复现）。"""
        v = self.get(version_id)
        return v.template_hash == template_hash and v.params_hash == params_hash
