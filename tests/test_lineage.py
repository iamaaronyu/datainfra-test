"""版本链与血缘登记测试 —— DataHub 替身（架构 §5.3，需求 §5）。"""
import pytest

from compute_platform.governance.lineage import LineageRegistry, LineageError


def _reg(reg, dataset, round, parents=(), region="guiyang", th="t0", ph="p0"):
    return reg.register(dataset=dataset, round=round, region=region, job_id=f"job-{round}",
                        model="qwen3.5-vl-235b", template_hash=th, params_hash=ph,
                        parents=parents)


def test_register_and_get():
    reg = LineageRegistry()
    v = _reg(reg, "clean-corpus", 0)
    assert reg.get(v.version_id) == v
    assert v.round == 0 and v.parents == ()


def test_round_chain_parent_must_exist():
    reg = LineageRegistry()
    with pytest.raises(LineageError):
        _reg(reg, "clean-corpus", 1, parents=("does-not-exist",))


def test_multi_round_version_chain():
    reg = LineageRegistry()
    v0 = _reg(reg, "corpus", 0)
    v1 = _reg(reg, "corpus", 1, parents=(v0.version_id,))
    v2 = _reg(reg, "corpus", 2, parents=(v1.version_id,))
    anc = {a.version_id for a in reg.ancestors(v2.version_id)}
    assert anc == {v0.version_id, v1.version_id}   # 端到端回溯跨轮


def test_latest_picks_highest_round():
    reg = LineageRegistry()
    _reg(reg, "corpus", 0)
    v1 = _reg(reg, "corpus", 1, parents=(reg.latest("corpus").version_id,))
    assert reg.latest("corpus").version_id == v1.version_id


def test_region_of_for_locality():
    reg = LineageRegistry()
    v = _reg(reg, "corpus", 0, region="shanghai")
    assert reg.region_of(v.version_id) == "shanghai"


def test_reproducibility_check():
    reg = LineageRegistry()
    v = _reg(reg, "corpus", 0, th="tmpl-abc", ph="param-xyz")
    assert reg.reproducible_with(v.version_id, "tmpl-abc", "param-xyz") is True
    assert reg.reproducible_with(v.version_id, "tmpl-abc", "other") is False


def test_cross_line_lineage_multiple_parents():
    reg = LineageRegistry()
    a = _reg(reg, "line-a", 0)
    b = _reg(reg, "line-b", 0)
    merged = _reg(reg, "merged", 1, parents=(a.version_id, b.version_id))
    anc = {x.version_id for x in reg.ancestors(merged.version_id)}
    assert anc == {a.version_id, b.version_id}
