"""分片队列接口（§4.4 扩展点：队列后端可插拔，Redis 与 PostgreSQL 互换）。

契约（§4.3）：claim / renew / commit / abort / fail，外加幂等入队、过期回收、
统计、死信管理。正确性只依赖会自动过期的租约，不依赖 worker 体面退出。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from ..models import Shard


@dataclass
class QueueStats:
    pending: int = 0
    running: int = 0
    done: int = 0
    dead: int = 0
    cancelled: int = 0

    @property
    def total(self) -> int:
        return self.pending + self.running + self.done + self.dead + self.cancelled

    @property
    def finished(self) -> bool:
        """完成定义（§6.3）：待领取 + 执行中清零；死信单独核销。"""
        return self.pending == 0 and self.running == 0


class ShardQueue(ABC):
    @abstractmethod
    def enqueue_many(self, shards: list[Shard]) -> int:
        """幂等入队（按 shard_id 去重，§6.2 切分可重入）。返回新插入数。"""

    @abstractmethod
    def claim(self, model: str, lease_seconds: int) -> Shard | None:
        """原子领取一个待领取分片；先回收过期租约。None = 没活了。"""

    @abstractmethod
    def renew(self, shard_id: str, token: str, lease_seconds: int) -> bool:
        """心跳续租（CAS：仅当租约仍属自己）。"""

    @abstractmethod
    def commit(self, shard_id: str, token: str) -> bool:
        """提交完成（CAS 防假死复活提交脏状态，§6.3）。"""

    @abstractmethod
    def abort(self, shard_id: str, token: str) -> bool:
        """放弃分片，释放回待领取（被抢占优雅退出用，不计重试）。"""

    @abstractmethod
    def fail(self, shard_id: str, token: str) -> bool:
        """确定性失败：重试 +1，超限转死信（毒分片，§6.3）。"""

    @abstractmethod
    def reap_expired(self) -> int:
        """回收所有过期租约：重试 +1 → 待领取，超限 → 死信。返回回收数。"""

    @abstractmethod
    def stats(self, job_id: str) -> QueueStats:
        ...

    @abstractmethod
    def cancel_job(self, job_id: str) -> int:
        """取消作业下未完成分片。返回受影响数。"""

    @abstractmethod
    def dead_letters(self, job_id: str) -> list[Shard]:
        ...

    @abstractmethod
    def retry_dead_letters(self, job_id: str) -> int:
        """死信重置为待领取、清零重试。返回重置数。"""
