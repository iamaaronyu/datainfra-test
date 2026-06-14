"""SQLite 实现的分片队列 + 租约（对应设计里的 Redis/PostgreSQL）。

PostgreSQL 版用 `UPDATE ... FOR UPDATE SKIP LOCKED`；这里用 SQLite 的
`BEGIN IMMEDIATE` 写事务保证 claim 原子。租约即一列 lease_expire_at。

时钟可注入（self.clock），便于测试无需真实 sleep 即可验证租约过期。
"""
from __future__ import annotations

import sqlite3
import time
import uuid

from ..models import Shard, ShardStatus
from .base import ShardQueue, QueueStats

_SCHEMA = """
CREATE TABLE IF NOT EXISTS shards (
    shard_id        TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL,
    model           TEXT NOT NULL,
    file            TEXT NOT NULL,
    start_offset    INTEGER NOT NULL,
    end_offset      INTEGER NOT NULL,
    rows            INTEGER NOT NULL,
    status          TEXT NOT NULL,
    retries         INTEGER NOT NULL DEFAULT 0,
    lease_owner     TEXT,
    lease_expire_at REAL
);
CREATE INDEX IF NOT EXISTS idx_shards_claim ON shards(model, status);
CREATE INDEX IF NOT EXISTS idx_shards_job   ON shards(job_id, status);
"""


class SqliteShardQueue(ShardQueue):
    def __init__(self, db_path: str, max_retries: int = 3):
        self.db_path = db_path
        self.max_retries = max_retries
        self.clock = time.time  # 可注入
        con = self._connect()
        con.executescript(_SCHEMA)
        con.commit()
        con.close()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=30000")
        return con

    @staticmethod
    def _row_to_shard(r: sqlite3.Row) -> Shard:
        return Shard(
            shard_id=r["shard_id"], job_id=r["job_id"], model=r["model"],
            file=r["file"], start_offset=r["start_offset"], end_offset=r["end_offset"],
            rows=r["rows"], status=ShardStatus(r["status"]), retries=r["retries"],
            lease_owner=r["lease_owner"], lease_expire_at=r["lease_expire_at"],
        )

    # ---- 写操作 ----

    def enqueue_many(self, shards: list[Shard]) -> int:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            before = con.total_changes
            con.executemany(
                "INSERT OR IGNORE INTO shards"
                "(shard_id, job_id, model, file, start_offset, end_offset, rows, status, retries)"
                " VALUES (?,?,?,?,?,?,?,?,0)",
                [(s.shard_id, s.job_id, s.model, s.file, s.start_offset,
                  s.end_offset, s.rows, ShardStatus.PENDING.value) for s in shards],
            )
            con.execute("COMMIT")
            return con.total_changes - before
        finally:
            con.close()

    def _reap_locked(self, con: sqlite3.Connection) -> int:
        """在已持写锁的连接内回收过期租约。"""
        now = self.clock()
        rows = con.execute(
            "SELECT shard_id, retries FROM shards"
            " WHERE status=? AND lease_expire_at IS NOT NULL AND lease_expire_at < ?",
            (ShardStatus.RUNNING.value, now),
        ).fetchall()
        n = 0
        for r in rows:
            if r["retries"] + 1 > self.max_retries:
                new_status = ShardStatus.DEAD.value
            else:
                new_status = ShardStatus.PENDING.value
            con.execute(
                "UPDATE shards SET status=?, retries=retries+1, lease_owner=NULL,"
                " lease_expire_at=NULL WHERE shard_id=?",
                (new_status, r["shard_id"]),
            )
            n += 1
        return n

    def reap_expired(self) -> int:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            n = self._reap_locked(con)
            con.execute("COMMIT")
            return n
        finally:
            con.close()

    def claim(self, model: str, lease_seconds: int) -> Shard | None:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            self._reap_locked(con)  # 先回收过期租约
            row = con.execute(
                "SELECT * FROM shards WHERE model=? AND status=?"
                " ORDER BY retries ASC, shard_id ASC LIMIT 1",
                (model, ShardStatus.PENDING.value),
            ).fetchone()
            if row is None:
                con.execute("COMMIT")
                return None
            token = uuid.uuid4().hex
            now = self.clock()
            con.execute(
                "UPDATE shards SET status=?, lease_owner=?, lease_expire_at=?"
                " WHERE shard_id=?",
                (ShardStatus.RUNNING.value, token, now + lease_seconds, row["shard_id"]),
            )
            con.execute("COMMIT")
            shard = self._row_to_shard(row)
            shard.status = ShardStatus.RUNNING
            shard.lease_owner = token
            shard.lease_expire_at = now + lease_seconds
            return shard
        finally:
            con.close()

    def _cas_update(self, sql: str, params: tuple) -> bool:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(sql, params)
            changed = cur.rowcount
            con.execute("COMMIT")
            return changed == 1
        finally:
            con.close()

    def renew(self, shard_id: str, token: str, lease_seconds: int) -> bool:
        return self._cas_update(
            "UPDATE shards SET lease_expire_at=? WHERE shard_id=? AND lease_owner=? AND status=?",
            (self.clock() + lease_seconds, shard_id, token, ShardStatus.RUNNING.value),
        )

    def commit(self, shard_id: str, token: str) -> bool:
        return self._cas_update(
            "UPDATE shards SET status=?, lease_owner=NULL, lease_expire_at=NULL"
            " WHERE shard_id=? AND lease_owner=? AND status=?",
            (ShardStatus.DONE.value, shard_id, token, ShardStatus.RUNNING.value),
        )

    def abort(self, shard_id: str, token: str) -> bool:
        return self._cas_update(
            "UPDATE shards SET status=?, lease_owner=NULL, lease_expire_at=NULL"
            " WHERE shard_id=? AND lease_owner=? AND status=?",
            (ShardStatus.PENDING.value, shard_id, token, ShardStatus.RUNNING.value),
        )

    def fail(self, shard_id: str, token: str) -> bool:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT retries FROM shards WHERE shard_id=? AND lease_owner=? AND status=?",
                (shard_id, token, ShardStatus.RUNNING.value),
            ).fetchone()
            if row is None:
                con.execute("COMMIT")
                return False
            new_status = (ShardStatus.DEAD.value
                          if row["retries"] + 1 > self.max_retries
                          else ShardStatus.PENDING.value)
            con.execute(
                "UPDATE shards SET status=?, retries=retries+1, lease_owner=NULL,"
                " lease_expire_at=NULL WHERE shard_id=?",
                (new_status, shard_id),
            )
            con.execute("COMMIT")
            return True
        finally:
            con.close()

    def cancel_job(self, job_id: str) -> int:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(
                "UPDATE shards SET status=?, lease_owner=NULL, lease_expire_at=NULL"
                " WHERE job_id=? AND status IN (?,?)",
                (ShardStatus.CANCELLED.value, job_id,
                 ShardStatus.PENDING.value, ShardStatus.RUNNING.value),
            )
            con.execute("COMMIT")
            return cur.rowcount
        finally:
            con.close()

    # ---- 读操作 ----

    def stats(self, job_id: str) -> QueueStats:
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT status, COUNT(*) c FROM shards WHERE job_id=? GROUP BY status",
                (job_id,),
            ).fetchall()
        finally:
            con.close()
        m = {r["status"]: r["c"] for r in rows}
        return QueueStats(
            pending=m.get(ShardStatus.PENDING.value, 0),
            running=m.get(ShardStatus.RUNNING.value, 0),
            done=m.get(ShardStatus.DONE.value, 0),
            dead=m.get(ShardStatus.DEAD.value, 0),
            cancelled=m.get(ShardStatus.CANCELLED.value, 0),
        )

    def dead_letters(self, job_id: str) -> list[Shard]:
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT * FROM shards WHERE job_id=? AND status=? ORDER BY shard_id",
                (job_id, ShardStatus.DEAD.value),
            ).fetchall()
        finally:
            con.close()
        return [self._row_to_shard(r) for r in rows]

    def retry_dead_letters(self, job_id: str) -> int:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(
                "UPDATE shards SET status=?, retries=0 WHERE job_id=? AND status=?",
                (ShardStatus.PENDING.value, job_id, ShardStatus.DEAD.value),
            )
            con.execute("COMMIT")
            return cur.rowcount
        finally:
            con.close()
