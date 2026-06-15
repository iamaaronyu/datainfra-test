"""prefix/KV cache 亲和路由（§6.4，Claude Code 专项）。"""
from inference_pool import CacheAffinityRouter, Priority, Request


def _req(session):
    return Request(request_id="x", priority=Priority.ONLINE, session_key=session)


def test_first_request_is_miss_then_hits():
    r = CacheAffinityRouter(capacity=10)
    assert r.route(_req("repo-A")) is False  # 冷会话，建立 pin
    assert r.route(_req("repo-A")) is True   # 后续命中 prefix cache
    assert r.route(_req("repo-A")) is True
    assert r.hit_rate == 2 / 3


def test_sticky_to_same_replica():
    r = CacheAffinityRouter(capacity=10)
    r.route(_req("s1"))
    idx = r._pin["s1"]
    for _ in range(5):
        r.route(_req("s1"))
    assert r._pin["s1"] == idx  # 始终粘在同一副本


def test_shrink_invalidates_only_removed_replicas():
    r = CacheAffinityRouter(capacity=10)
    # 造一个落在高位副本的会话。
    key = next(k for k in ("a", "b", "c", "d", "e", "f")
               if (hash(k) & 0x7FFFFFFF) % 10 >= 5)
    r.route(_req(key))
    assert r._pin[key] >= 5
    r.set_capacity(5)  # 缩容，移除副本 5..9
    assert key not in r._pin           # 该会话亲和失效
    assert r.route(_req(key)) is False  # 重新落位为 miss


def test_no_session_key_no_affinity():
    r = CacheAffinityRouter(capacity=10)
    req = Request(request_id="x", priority=Priority.ONLINE, session_key=None)
    assert r.route(req) is False
    assert r.hits == 0
