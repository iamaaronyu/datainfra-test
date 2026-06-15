"""推理混跑层：在线优先级插队、批量填谷、不搬卡（§5.2 / §6.1）。"""
from inference_pool import InferenceGateway, Priority, Request


def _req(i, pri, model="GLM5.1", session=None):
    return Request(request_id=f"r{i}", priority=pri, model=model, session_key=session)


def test_online_served_before_batch(gateway):
    gateway.set_online_cards(1)  # 1 卡 × 4 slot = 4 slot
    for i in range(2):
        gateway.submit(_req(f"o{i}", Priority.ONLINE))
    for i in range(10):
        gateway.submit(_req(f"b{i}", Priority.BATCH))
    res = gateway.step()
    # 2 个在线先排满，剩 2 slot 给批量填谷。
    assert len(res.served_online) == 2
    assert len(res.served_batch) == 2


def test_batch_fills_idle_slots(gateway):
    """在线有空泡时，批量自动填满（§5.2 核心日常）。"""
    gateway.set_online_cards(2)  # 8 slot
    gateway.submit(_req("o0", Priority.ONLINE))
    for i in range(20):
        gateway.submit(_req(f"b{i}", Priority.BATCH))
    res = gateway.step()
    assert len(res.served_online) == 1
    assert len(res.served_batch) == 7  # 8 - 1


def test_online_never_starved_by_batch(gateway):
    """无论批量多少，在线请求当步即被服务（在线 SLA 不被批量拖累，R1.3）。"""
    gateway.set_online_cards(3)  # 12 slot
    for i in range(50):
        gateway.submit(_req(f"b{i}", Priority.BATCH))
    for i in range(12):
        gateway.submit(_req(f"o{i}", Priority.ONLINE))
    res = gateway.step()
    assert len(res.served_online) == 12
    assert res.online_waiting == 0


def test_batch_paused_at_peak(gateway):
    """高峰可暂停批量填谷（R1.4）。"""
    gateway.set_online_cards(2)
    gateway.pause_batch(True)
    gateway.submit(_req("o0", Priority.ONLINE))
    for i in range(5):
        gateway.submit(_req(f"b{i}", Priority.BATCH))
    res = gateway.step()
    assert len(res.served_online) == 1
    assert res.served_batch == []


def test_online_overflow_signals_pressure(gateway):
    """在线需求超过 slot → online_waiting>0，作为扩容信号（喂给 autoscaler）。"""
    gateway.set_online_cards(1)  # 4 slot
    for i in range(10):
        gateway.submit(_req(f"o{i}", Priority.ONLINE))
    res = gateway.step()
    assert len(res.served_online) == 4
    assert res.online_waiting == 6
    assert gateway.metrics().queue_depth == 6
