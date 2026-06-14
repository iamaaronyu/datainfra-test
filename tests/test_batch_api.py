import pytest
from fastapi.testclient import TestClient

from compute_platform.batch_api import BatchService, create_app
from compute_platform.governance import QuotaManager
from compute_platform.worker import Worker
from compute_platform.inference import MockEngine


@pytest.fixture
def client(config, store, queue, registry, job_store, dataset):
    config.target_shard_seconds = 1  # 多分片
    dataset(key="datasets/input.jsonl", n=200)
    quota = QuotaManager({"team-a": 64, "tiny": 4})
    service = BatchService(config, store, queue, registry, job_store, quota)
    app = create_app(service)
    return TestClient(app), service


def _submit(c, model="Qwen3.5-VL-235B", token=None, tenant="team-a"):
    body = {"model": model, "input_key": "datasets/input.jsonl",
            "output_prefix": "out/job", "prompt_template": "summarize: {text}"}
    if token:
        body["client_token"] = token
    return c.post("/v1/batch/jobs", json=body, headers={"X-Tenant-Id": tenant})


def test_submit_returns_job(client):
    c, _ = client
    r = _submit(c)
    assert r.status_code == 200
    body = r.json()
    assert body["job_id"].startswith("job-")
    assert body["status"] == "running"
    assert body["total_shards"] == 10   # 200 行 / 20


def test_submit_unknown_model_400(client):
    c, _ = client
    r = c.post("/v1/batch/jobs", json={
        "model": "no-such-model", "input_key": "datasets/input.jsonl",
        "output_prefix": "out/x"}, headers={"X-Tenant-Id": "team-a"})
    assert r.status_code == 400


def test_submit_missing_input_400(client):
    c, _ = client
    r = c.post("/v1/batch/jobs", json={
        "model": "small-7B", "input_key": "datasets/missing.jsonl",
        "output_prefix": "out/x"}, headers={"X-Tenant-Id": "team-a"})
    assert r.status_code == 400


def test_client_token_idempotent(client):
    c, _ = client
    r1 = _submit(c, token="tok-123")
    r2 = _submit(c, token="tok-123")
    assert r1.json()["job_id"] == r2.json()["job_id"]


def test_quota_insufficient_429(client):
    c, _ = client
    # tenant tiny 配额 4 卡，235B 需 8 卡 -> 拒绝
    r = _submit(c, tenant="tiny")
    assert r.status_code == 429


def test_progress_and_completion(client):
    c, service = client
    job_id = _submit(c).json()["job_id"]
    # 起一个 worker 把它跑完
    w = Worker("w", service.queue, service.store, MockEngine("Qwen3.5-VL-235B"),
               "Qwen3.5-VL-235B", "out/job")
    w.run()
    r = c.get(f"/v1/batch/jobs/{job_id}")
    body = r.json()
    assert body["status"] == "succeeded"
    assert body["progress"] == 1.0
    assert body["done"] == 10


def test_errors_and_retry(client, dataset):
    c, service = client
    # 投毒数据集
    dataset(key="datasets/poison.jsonl", n=200, poison_at={5})
    r = c.post("/v1/batch/jobs", json={
        "model": "Qwen3.5-VL-235B", "input_key": "datasets/poison.jsonl",
        "output_prefix": "out/poison"}, headers={"X-Tenant-Id": "team-a"})
    job_id = r.json()["job_id"]
    w = Worker("w", service.queue, service.store, MockEngine("Qwen3.5-VL-235B"),
               "Qwen3.5-VL-235B", "out/poison", batch_size=8)
    w.run()
    # 一个毒分片进死信
    err = c.get(f"/v1/batch/jobs/{job_id}/errors").json()
    assert len(err["dead_letters"]) == 1
    prog = c.get(f"/v1/batch/jobs/{job_id}").json()
    assert prog["status"] == "completed_with_errors"
    # 重试死信
    rr = c.post(f"/v1/batch/jobs/{job_id}/retry").json()
    assert rr["requeued"] == 1


def test_cancel(client):
    c, _ = client
    job_id = _submit(c).json()["job_id"]
    r = c.post(f"/v1/batch/jobs/{job_id}/cancel").json()
    assert r["cancelled_shards"] == 10
    prog = c.get(f"/v1/batch/jobs/{job_id}").json()
    assert prog["status"] == "cancelled"


def test_status_404(client):
    c, _ = client
    assert c.get("/v1/batch/jobs/nope").status_code == 404
