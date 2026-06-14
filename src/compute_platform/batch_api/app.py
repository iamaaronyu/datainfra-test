"""离线批量任务平台 REST API（§6.2）。

五个接口：提交 / 查询 / 错误明细 / 取消 / 重试。
租户由 X-Tenant-Id 头标识（缺省 default），对应设计的多租户逻辑隔离。
"""
from __future__ import annotations

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from ..models import QoS
from .service import BatchService, SubmitRequest, ValidationError


class SubmitBody(BaseModel):
    model: str
    input_key: str
    output_prefix: str
    prompt_template: str = ""
    qos: QoS = QoS.PREEMPTIBLE
    client_token: str | None = None


def create_app(service: BatchService) -> FastAPI:
    app = FastAPI(title="离线批量任务平台", version="1.0.0")

    def _tenant(x_tenant_id: str | None) -> str:
        return x_tenant_id or "default"

    @app.post("/v1/batch/jobs")
    def submit(body: SubmitBody, x_tenant_id: str | None = Header(default=None)):
        try:
            job = service.submit(_tenant(x_tenant_id), SubmitRequest(
                model=body.model, input_key=body.input_key,
                output_prefix=body.output_prefix, prompt_template=body.prompt_template,
                qos=body.qos, client_token=body.client_token))
        except ValidationError as e:
            raise HTTPException(status_code=e.code, detail=e.message)
        return {"job_id": job.job_id, "status": job.status.value,
                "total_shards": job.total_shards}

    @app.get("/v1/batch/jobs/{job_id}")
    def status(job_id: str):
        try:
            return service.progress(job_id)
        except ValidationError as e:
            raise HTTPException(status_code=e.code, detail=e.message)

    @app.get("/v1/batch/jobs/{job_id}/errors")
    def errors(job_id: str):
        try:
            return {"job_id": job_id, "dead_letters": service.errors(job_id)}
        except ValidationError as e:
            raise HTTPException(status_code=e.code, detail=e.message)

    @app.post("/v1/batch/jobs/{job_id}/cancel")
    def cancel(job_id: str):
        try:
            return {"job_id": job_id, "cancelled_shards": service.cancel(job_id)}
        except ValidationError as e:
            raise HTTPException(status_code=e.code, detail=e.message)

    @app.post("/v1/batch/jobs/{job_id}/retry")
    def retry(job_id: str):
        try:
            return {"job_id": job_id, "requeued": service.retry(job_id)}
        except ValidationError as e:
            raise HTTPException(status_code=e.code, detail=e.message)

    return app
