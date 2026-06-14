"""BatchService 的 v2.0 治理接线测试：数据局部性 + task×DataHub 血缘闭环。"""
import pytest

from compute_platform.batch_api.service import BatchService, SubmitRequest, ValidationError
from compute_platform.governance import QuotaManager, LineageRegistry
from compute_platform.models import JobStatus


def _service(config, store, queue, registry, job_store, *, lineage=None, ray_clusters=None):
    return BatchService(config, store, queue, registry, job_store,
                        quota=QuotaManager({"data-team": 256}),
                        lineage=lineage, ray_clusters=ray_clusters)


def test_locality_rejects_when_local_cluster_down(config, store, queue, registry, job_store, dataset):
    key, _ = dataset(key="datasets/in.jsonl", n=50)
    svc = _service(config, store, queue, registry, job_store,
                   ray_clusters={"guiyang": False, "shanghai": True})
    req = SubmitRequest(model="small-7B", input_key=key, output_prefix="out",
                        dataset_region="guiyang")
    with pytest.raises(ValidationError) as ei:
        svc.submit("data-team", req)
    assert ei.value.code == 409


def test_locality_allows_when_colocated(config, store, queue, registry, job_store, dataset):
    key, _ = dataset(key="datasets/in.jsonl", n=50)
    svc = _service(config, store, queue, registry, job_store,
                   ray_clusters={"guiyang": True})
    req = SubmitRequest(model="small-7B", input_key=key, output_prefix="out",
                        dataset_region="guiyang")
    job = svc.submit("data-team", req)
    assert job.status == JobStatus.RUNNING


def test_submit_rejects_missing_parent_version(config, store, queue, registry, job_store, dataset):
    key, _ = dataset(key="datasets/in.jsonl", n=50)
    reg = LineageRegistry()
    svc = _service(config, store, queue, registry, job_store, lineage=reg)
    req = SubmitRequest(model="small-7B", input_key=key, output_prefix="out",
                        input_versions=("nope",))
    with pytest.raises(Exception):  # LineageError 冒泡
        svc.submit("data-team", req)


def test_lineage_round_chain_closure(config, store, queue, registry, job_store, dataset):
    """两轮：round0 产出 → round1 以其为 parent，完成时回写血缘成链。"""
    key, _ = dataset(key="datasets/in.jsonl", n=50)
    reg = LineageRegistry()
    svc = _service(config, store, queue, registry, job_store, lineage=reg)

    # round 0
    job0 = svc.submit("data-team", SubmitRequest(
        model="small-7B", input_key=key, output_prefix="out0",
        dataset="corpus", round=0, dataset_region="guiyang",
        prompt_template="v1", client_token="r0"))
    _drain(queue, store, job0)
    v0 = svc.complete(job0.job_id, params_hash="temp=0")
    assert v0 is not None and v0.round == 0

    # round 1，输入 = round0 输出版本
    job1 = svc.submit("data-team", SubmitRequest(
        model="small-7B", input_key=key, output_prefix="out1",
        dataset="corpus", round=1, dataset_region="guiyang",
        input_versions=(v0.version_id,), prompt_template="v2", client_token="r1"))
    _drain(queue, store, job1)
    v1 = svc.complete(job1.job_id, params_hash="temp=0")

    assert v1.parents == (v0.version_id,)
    anc = {a.version_id for a in reg.ancestors(v1.version_id)}
    assert anc == {v0.version_id}
    assert reg.latest("corpus").version_id == v1.version_id


def test_complete_blocked_until_finished(config, store, queue, registry, job_store, dataset):
    key, _ = dataset(key="datasets/in.jsonl", n=50)
    reg = LineageRegistry()
    svc = _service(config, store, queue, registry, job_store, lineage=reg)
    job = svc.submit("data-team", SubmitRequest(
        model="small-7B", input_key=key, output_prefix="out",
        dataset="corpus", round=0, dataset_region="guiyang"))
    # 未跑完就 complete → 409
    with pytest.raises(ValidationError) as ei:
        svc.complete(job.job_id)
    assert ei.value.code == 409


def _drain(queue, store, job):
    """把作业所有分片直接置 done（模拟 worker 跑完），驱动 progress→SUCCEEDED。"""
    from compute_platform.inference import MockEngine
    from compute_platform.worker import Worker
    w = Worker("w-test", queue, store, MockEngine(job.model), job.model, "out-drain")
    w.engine.load()
    while w.run_once():
        pass
