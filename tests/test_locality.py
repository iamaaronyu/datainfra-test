"""数据局部性硬约束测试（架构 §5.1）。"""
import pytest

from compute_platform.scheduler.locality import (
    resolve_placement,
    Placement,
    LocalityViolation,
)


def test_colocate_with_data():
    p = resolve_placement("guiyang", {"guiyang": True, "shanghai": True})
    assert isinstance(p, Placement)
    assert p.region == "guiyang"
    assert p.cross_region is False


def test_local_cluster_down_blocks_cross_region_by_default():
    with pytest.raises(LocalityViolation):
        resolve_placement("guiyang", {"guiyang": False, "shanghai": True})


def test_cross_region_allowed_when_explicit():
    p = resolve_placement(
        "guiyang", {"guiyang": False, "shanghai": True}, allow_cross_region=True)
    assert p.region == "shanghai"
    assert p.cross_region is True


def test_missing_region_metadata_rejected():
    with pytest.raises(LocalityViolation):
        resolve_placement("", {"guiyang": True})


def test_no_cluster_available_even_with_crossregion():
    with pytest.raises(LocalityViolation):
        resolve_placement(
            "guiyang", {"guiyang": False, "shanghai": False}, allow_cross_region=True)
