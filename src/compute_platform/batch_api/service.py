"""离线作业接入服务（接入面，§6.2）。

服务化边界 = 整个任务提交（不是每次推理调用，§6.1）。
submit 做轻校验 → 创建作业 → 切分入队 → 返回 job_id；client_token 幂等。
JobStore 用 sqlite 持久化作业元数据（与分片队列同库不同表）。
"""
from __future__ import annotations

import hashlib
import sqlite3
import time
import uuid
from dataclasses import dataclass

from ..config import Config
from ..governance.quota import QuotaManager, QuotaExceeded
from ..models import Job, JobStatus, QoS
from ..objectstore import LocalObjectStore
from ..queue.base import ShardQueue
from ..registry import ModelRegistry
from ..sharder import Sharder


class ValidationError(Exception):
    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


@dataclass
class SubmitRequest:
    model: str
    input_key: str
    output_prefix: str
    prompt_template: str = ""
    qos: QoS = QoS.PREEMPTIBLE
    client_token: str | None = None


_JOBS_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id        TEXT PRIMARY KEY,
    tenant        TEXT NOT NULL,
    model         TEXT NOT NULL,
    input_key     TEXT NOT NULL,
    output_prefix TEXT NOT NULL,
    template_hash TEXT NOT NULL,
    status        TEXT NOT NULL,
    total_shards  INTEGER NOT NULL DEFAULT 0,
    client_token  TEXT,
    qos           TEXT NOT NULL,
    created_at    REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_token ON jobs(tenant, client_token)
    WHERE client_token IS NOT NULL;
"""


class JobStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        con = self._connect()
        con.executescript(_JOBS_SCHEMA)
        con.commit()
        con.close()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=30000")
        return con

    def find_by_token(self, tenant: str, token: str) -> Job | None:
        con = self._connect()
        try:
            r = con.execute(
                "SELECT * FROM jobs WHERE tenant=? AND client_token=?",
                (tenant, token)).fetchone()
        finally:
            con.close()
        return self._row(r) if r else None

    def insert(self, job: Job) -> None:
        con = self._connect()
        try:
            con.execute(
                "INSERT INTO jobs(job_id, tenant, model, input_key, output_prefix,"
                " template_hash, status, total_shards, client_token, qos, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (job.job_id, job.tenant, job.model, job.input_key, job.output_prefix,
                 job.template_hash, job.status.value, job.total_shards,
                 job.client_token, job.qos.value, job.created_at))
            con.commit()
        finally:
            con.close()

    def update(self, job_id: str, **fields) -> None:
        if not fields:
            return
        cols = ", ".join(f"{k}=?" for k in fields)
        vals = list(fields.values()) + [job_id]
        con = self._connect()
        try:
            con.execute(f"UPDATE jobs SET {cols} WHERE job_id=?", vals)
            con.commit()
        finally:
            con.close()

    def get(self, job_id: str) -> Job | None:
        con = self._connect()
        try:
            r = con.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        finally:
            con.close()
        return self._row(r) if r else None

    @staticmethod
    def _row(r: sqlite3.Row) -> Job:
        return Job(
            job_id=r["job_id"], tenant=r["tenant"], model=r["model"],
            input_key=r["input_key"], output_prefix=r["output_prefix"],
            template_hash=r["template_hash"], status=JobStatus(r["status"]),
            total_shards=r["total_shards"], client_token=r["client_token"],
            qos=QoS(r["qos"]), created_at=r["created_at"])


class BatchService:
    def __init__(self, config: Config, store: LocalObjectStore, queue: ShardQueue,
                 registry: ModelRegistry, job_store: JobStore,
                 quota: QuotaManager | None = None):
        self.config = config
        self.store = store
        self.queue = queue
        self.registry = registry
        self.jobs = job_store
        self.quota = quota
        self.sharder = Sharder(store, config)

    def submit(self, tenant: str, req: SubmitRequest) -> Job:
        # 幂等：同 client_token 直接返回已有作业
        if req.client_token:
            existing = self.jobs.find_by_token(tenant, req.client_token)
            if existing:
                return existing

        # 轻校验（§6.2）
        if not self.registry.exists(req.model):
            raise ValidationError(400, f"unknown model: {req.model}")
        if not self.store.exists(req.input_key):
            raise ValidationError(400, f"input not readable: {req.input_key}")
        spec = self.registry.get(req.model)
        if self.quota is not None:
            if self.quota.available(tenant) < spec.cards_per_worker:
                raise ValidationError(
                    429, f"quota insufficient for tenant={tenant}")

        template_hash = hashlib.sha256(req.prompt_template.encode()).hexdigest()[:16]
        job = Job(
            job_id="job-" + uuid.uuid4().hex[:12],
            tenant=tenant, model=req.model, input_key=req.input_key,
            output_prefix=req.output_prefix, template_hash=template_hash,
            status=JobStatus.SPLITTING, client_token=req.client_token,
            qos=req.qos, created_at=time.time())
        self.jobs.insert(job)

        # 切分入队（MVP 同步切分；亿级可异步化）
        shards = self.sharder.split(job.job_id, req.model, req.input_key, spec)
        self.queue.enqueue_many(shards)
        self.jobs.update(job.job_id, status=JobStatus.RUNNING.value,
                         total_shards=len(shards))
        job.status = JobStatus.RUNNING
        job.total_shards = len(shards)
        return job

    def progress(self, job_id: str) -> dict:
        job = self.jobs.get(job_id)
        if job is None:
            raise ValidationError(404, "job not found")
        st = self.queue.stats(job_id)
        # 完成态推导（§6.2 状态机）
        status = job.status
        if job.status not in (JobStatus.CANCELLED, JobStatus.FAILED):
            if st.finished and st.total > 0:
                status = (JobStatus.COMPLETED_WITH_ERRORS if st.dead > 0
                          else JobStatus.SUCCEEDED)
                if status != job.status:
                    self.jobs.update(job_id, status=status.value)
        total = job.total_shards or st.total
        return {
            "job_id": job_id,
            "status": status.value,
            "total_shards": total,
            "done": st.done,
            "running": st.running,
            "pending": st.pending,
            "dead": st.dead,
            "cancelled": st.cancelled,
            "progress": round(st.done / total, 4) if total else 0.0,
        }

    def errors(self, job_id: str) -> list[dict]:
        if self.jobs.get(job_id) is None:
            raise ValidationError(404, "job not found")
        return [{"shard_id": s.shard_id, "rows": s.rows, "retries": s.retries}
                for s in self.queue.dead_letters(job_id)]

    def cancel(self, job_id: str) -> int:
        if self.jobs.get(job_id) is None:
            raise ValidationError(404, "job not found")
        n = self.queue.cancel_job(job_id)
        self.jobs.update(job_id, status=JobStatus.CANCELLED.value)
        return n

    def retry(self, job_id: str) -> int:
        if self.jobs.get(job_id) is None:
            raise ValidationError(404, "job not found")
        n = self.queue.retry_dead_letters(job_id)
        if n > 0:
            self.jobs.update(job_id, status=JobStatus.RUNNING.value)
        return n
