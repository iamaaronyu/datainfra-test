import pytest

from compute_platform.objectstore import LocalObjectStore


def test_atomic_write_and_read(store: LocalObjectStore):
    store.write("a/b/c.txt", b"hello")
    assert store.read("a/b/c.txt") == b"hello"
    assert store.exists("a/b/c.txt")
    assert store.size("a/b/c.txt") == 5


def test_no_tmp_left_behind(store: LocalObjectStore):
    store.write("x.bin", b"0123456789")
    # 写完不应残留 .tmp
    assert store.list("") == ["x.bin"]


def test_read_range(store: LocalObjectStore):
    store.write("d.txt", b"0123456789")
    assert store.read_range("d.txt", 2, 5) == b"234"
    assert store.read_range("d.txt", 0, 10) == b"0123456789"


def test_overwrite_is_idempotent(store: LocalObjectStore):
    store.write("k", b"v1")
    store.write("k", b"v2")
    assert store.read("k") == b"v2"


def test_key_escape_blocked(store: LocalObjectStore):
    with pytest.raises(ValueError):
        store.write("../escape.txt", b"x")


def test_list_prefix(store: LocalObjectStore):
    store.write("out/1.done", b"a")
    store.write("out/2.done", b"b")
    store.write("other/3.done", b"c")
    assert store.list("out") == ["out/1.done", "out/2.done"]
