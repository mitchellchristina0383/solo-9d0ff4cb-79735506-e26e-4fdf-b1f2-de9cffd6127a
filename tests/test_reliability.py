"""加载可靠性告警与样本语义的端到端自测。

覆盖：
- 逾期样本冻结：超时后对账补确认成功，样本仍为逾期，不新增样本；
- 轮询/手动对账/迟到回执不重复计数，失败重试的新轮次另算；
- 告警触发、观察窗变空不算恢复、足量新近样本低于阈值才恢复；
- 显存预留/部署状态不受可靠性统计影响；
- 平台重启后告警与样本依据一致（同一 SQLite 库重开）。
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import pytest

TEST_DIR = f"/tmp/platform-test-rel-{uuid.uuid4().hex[:8]}"
# 与 tests/test_platform.py 一起运行时，app.config 可能已被先导入（配置在导入时固化）；
# 此时沿用已生效的配置与库路径，避免两个测试模块互相污染。
if "app.config" not in sys.modules:
    os.environ.setdefault("PLATFORM_DB", os.path.join(TEST_DIR, "test.db"))
    os.environ.setdefault(
        "PLATFORM_NODES",
        "node-a|模拟节点A|http://testnode-a:9001|8192,"
        "node-b|模拟节点B|http://testnode-b:9002|8192",
    )
    os.environ.setdefault("NODE_CALL_TIMEOUT", "1.0")
    os.environ.setdefault("LOAD_TIMEOUT", "3")
    os.environ.setdefault("POLL_INTERVAL", "0.2")
    os.environ.setdefault("HEALTH_INTERVAL", "3600")

from app import config, db, reliability, service  # noqa: E402
from app.main import app as platform_app  # noqa: E402
from node import simulator  # noqa: E402


class RoutingTransport(httpx.AsyncBaseTransport):
    def __init__(self, sim_a, sim_b):
        self._a = httpx.ASGITransport(app=sim_a)
        self._b = httpx.ASGITransport(app=sim_b)
        self._p = httpx.ASGITransport(app=platform_app)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host == "testnode-a":
            return await self._a.handle_async_request(request)
        if host == "testnode-b":
            return await self._b.handle_async_request(request)
        return await self._p.handle_async_request(request)


async def make_client():
    sim_a = simulator.create_app("node-a", "模拟节点A", 8192, 0.2, 0.1, unreachable_sleep=0)
    sim_b = simulator.create_app("node-b", "模拟节点B", 8192, 0.2, 0.1, unreachable_sleep=0)
    transport = RoutingTransport(sim_a, sim_b)
    c = httpx.AsyncClient(base_url="http://platform", transport=transport, timeout=10)
    return c, transport, sim_a, sim_b


def reset_db():
    from app import db as db_mod
    db_mod._conn = None
    db_path = config.DB_PATH
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(db_path + suffix)
        except FileNotFoundError:
            pass
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    service.init_db()


async def start_loops():
    async def poll_loop():
        while True:
            await service.poll_loading()
            await service.poll_releasing()
            await asyncio.sleep(0.2)

    async def health_loop():
        while True:
            await service.health_monitor_once()
            await asyncio.sleep(0.5)

    return [asyncio.create_task(poll_loop()), asyncio.create_task(health_loop())]


@pytest.fixture
async def client():
    reset_db()
    c, transport, sim_a, sim_b = await make_client()
    service.NODE_RUNTIME.clear()
    for n in config.NODES:
        service.NODE_RUNTIME[n.node_id] = {"online": None, "last_check": None,
                                           "detail": "尚未探测", "mode": ""}
    await service.node_client.close()
    service.node_client._client = httpx.AsyncClient(
        base_url="http://platform", transport=transport, timeout=1.0
    )
    tasks = await start_loops()
    try:
        yield c
    finally:
        for t in tasks:
            t.cancel()
        await service.node_client.close()


async def wait_for(client, dep_id, statuses, timeout=12.0):
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while asyncio.get_event_loop().time() < deadline:
        last = (await client.get(f"/api/deployments/{dep_id}")).json()
        if last["status"] in statuses:
            return last
        await asyncio.sleep(0.2)
    raise AssertionError(f"未进入 {statuses}，最后 {last and last['status']}")


async def wait_until(fn, timeout=10.0, interval=0.2):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        result = await fn()
        if result:
            return result
        await asyncio.sleep(interval)
    return None


def set_thresholds(node_id, window=300, min_samples=2, ratio=0.5):
    with db.tx() as conn:
        reliability.save_settings(conn, node_id, reliability.validate_settings(
            window, min_samples, ratio))
    reliability.evaluate_all()


def samples_for(node_id):
    return db.query("SELECT * FROM load_samples WHERE node_id=? ORDER BY id", (node_id,))


def alert_of(body, node_id):
    return next(n["alert"] for n in body["nodes"] if n["node_id"] == node_id)


# ---------------------------------------------------------------- 用例
@pytest.mark.asyncio
async def test_01_reliability_endpoint_and_settings(client):
    body = (await client.get("/api/reliability")).json()
    assert {n["node_id"] for n in body["nodes"]} == {"node-a", "node-b"}
    assert all(n["alert"]["status"] == "OK" for n in body["nodes"])

    r = await client.put("/api/nodes/node-a/alert-settings", json={
        "window_seconds": 120, "min_samples": 4, "abnormal_ratio": 0.25})
    assert r.status_code == 200, r.text
    assert r.json()["config"]["min_samples"] == 4
    assert r.json()["config"]["abnormal_ratio"] == 0.25

    bad = await client.put("/api/nodes/node-a/alert-settings", json={
        "window_seconds": 0, "min_samples": 4, "abnormal_ratio": 0.25})
    assert bad.status_code == 422
    missing = await client.put("/api/nodes/nope/alert-settings", json={
        "window_seconds": 120, "min_samples": 4, "abnormal_ratio": 0.25})
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_02_overdue_sample_frozen_after_successful_reconcile(client):
    """加载逾期后即使对账确认成功，这次样本仍算逾期；且只算一条。"""
    set_thresholds("node-a", min_samples=1, ratio=0.5)
    await client.post("/api/nodes/node-a/control", json={"mode": "unreachable"})
    await service.health_monitor_once()

    r = await client.post("/api/deployments", json={
        "model_name": "late", "mem_per_replica_mb": 512, "replica_count": 1})
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"UNVERIFIED"}, timeout=10)
    rep = dep["replicas"][0]
    assert rep["node_id"] == "node-a" and rep["status"] == "TIMEOUT"

    samples = samples_for("node-a")
    assert len(samples) == 1 and samples[0]["replica_id"] == rep["id"]
    assert samples[0]["status"] == "OVERDUE"

    # 节点其实加载完成：写入 loaded 再恢复，对账补确认成功
    sim = client._transport._a.app.state  # type: ignore[attr-defined]
    sim.replicas[rep["id"]] = {"model": "late", "mem_mb": 512,
                               "epoch": rep["epoch"], "status": "loaded"}
    await client.post("/api/nodes/node-a/control", json={"mode": "normal"})
    await service.health_monitor_once()
    for _ in range(6):
        await client.post(f"/api/deployments/{dep_id}/reconcile")
        dep = (await client.get(f"/api/deployments/{dep_id}")).json()
        if dep["replicas"][0]["status"] == "LOADED":
            break
        await asyncio.sleep(0.2)
    assert dep["replicas"][0]["status"] == "LOADED"  # 部署恢复可用

    # 样本仍为逾期，且只有那一条（对账/轮询/迟到回执不重复计数）
    samples = samples_for("node-a")
    assert len(samples) == 1
    assert samples[0]["status"] == "OVERDUE"
    reliability.evaluate_all()
    body = (await client.get("/api/reliability")).json()
    a = alert_of(body, "node-a")
    assert a["overdue_count"] == 1 and a["on_time_count"] == 0
    assert a["status"] == "ALERT"
    node = next(n for n in body["nodes"] if n["node_id"] == "node-a")
    # 告警列示对应的副本与轮次
    assert [(x["replica_id"], x["epoch"], x["status"])
            for x in node["abnormal_samples"]] == [(rep["id"], 1, "OVERDUE")]


@pytest.mark.asyncio
async def test_03_interleaved_reconcile_polls_do_not_duplicate(client):
    """并发手动对账 + 后台轮询 + 重复查询，样本数只随加载下发增长。"""
    r = await client.post("/api/deployments", json={
        "model_name": "steady", "mem_per_replica_mb": 256, "replica_count": 2})
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"AVAILABLE"})
    assert len(samples_for("node-a")) == 1 and len(samples_for("node-b")) == 1
    assert {x["status"] for x in dep["replicas"]} == {"LOADED"}

    await asyncio.gather(*[
        client.post(f"/api/deployments/{dep_id}/reconcile") for _ in range(5)
    ])
    await asyncio.sleep(1.0)
    assert len(samples_for("node-a")) == 1
    assert len(samples_for("node-b")) == 1
    assert {s["status"] for s in samples_for("node-a")} == {"ON_TIME"}


@pytest.mark.asyncio
async def test_04_failed_retry_creates_new_sample(client):
    """失败重试的新轮次另算一条样本：旧轮 FAILED + 新轮 ON_TIME。"""
    await client.post("/api/nodes/node-a/control", json={"load_fail": True})
    r = await client.post("/api/deployments", json={
        "model_name": "flaky", "mem_per_replica_mb": 256, "replica_count": 1})
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"FAILED"})
    rep1 = dep["replicas"][0]
    assert rep1["node_id"] == "node-a"
    s1 = samples_for("node-a")
    assert len(s1) == 1 and s1[0]["status"] == "FAILED"

    await client.post("/api/nodes/node-a/control",
                      json={"load_fail": False, "mode": "normal"})
    assert (await client.post(f"/api/deployments/{dep_id}/retry")).status_code == 200
    dep = await wait_for(client, dep_id, {"AVAILABLE"})
    assert dep["epoch"] == 2
    s2 = samples_for("node-a")
    assert len(s2) == 2
    assert s2[0]["status"] == "FAILED" and s2[1]["status"] == "ON_TIME"
    assert s2[1]["epoch"] == 2 and s2[1]["replica_id"] != s1[0]["replica_id"]


@pytest.mark.asyncio
async def test_05_empty_window_is_not_recovery(client):
    """告警后窗口变空不能算恢复；足量新近正常样本低于阈值才恢复。"""
    set_thresholds("node-a", window=60, min_samples=2, ratio=0.5)
    # 两次失败样本 -> 触发
    for _ in range(2):
        await client.post("/api/nodes/node-a/control", json={"load_fail": True})
        r = await client.post("/api/deployments", json={
            "model_name": "bad", "mem_per_replica_mb": 128, "replica_count": 1})
        await wait_for(client, r.json()["id"], {"FAILED"})
    reliability.evaluate_all()
    body = (await client.get("/api/reliability")).json()
    a = alert_of(body, "node-a")
    assert a["status"] == "ALERT" and a["failed_count"] == 2

    # 把这两条样本推到观察窗外：窗口为空，绝不能算恢复
    old = reliability.iso(datetime.now(timezone.utc) - timedelta(seconds=300))
    with db.tx() as conn:
        conn.execute("UPDATE load_samples SET dispatched_at=?, deadline_at=? WHERE node_id=?",
                     (old, old, "node-a"))
    reliability.evaluate_all()
    body = (await client.get("/api/reliability")).json()
    a = alert_of(body, "node-a")
    assert a["status"] == "ALERT", a
    assert a["sample_count"] == 0
    assert "不能算恢复" in a["detail"]

    # 补足新近的正常样本（2 条按时确认，异常率 0 < 0.5）才恢复。
    # 用双副本部署保证 node-a 每轮都分到一份，不依赖单副本的节点选择顺序。
    await client.post("/api/nodes/node-a/control",
                      json={"load_fail": False, "mode": "normal"})
    for i in range(2):
        r = await client.post("/api/deployments", json={
            "model_name": f"ok{i}", "mem_per_replica_mb": 128, "replica_count": 2})
        dep = await wait_for(client, r.json()["id"], {"AVAILABLE"})
        assert "node-a" in {x["node_id"] for x in dep["replicas"]}
    reliability.evaluate_all()
    body = (await client.get("/api/reliability")).json()
    a = alert_of(body, "node-a")
    assert a["status"] == "OK", a
    assert a["recovered_at"] and a["on_time_count"] == 2


@pytest.mark.asyncio
async def test_06_insufficient_new_samples_does_not_recover(client):
    """异常样本滑出窗口但新近有效样本不足最低数：维持告警。"""
    set_thresholds("node-a", window=60, min_samples=3, ratio=0.5)
    for _ in range(3):
        await client.post("/api/nodes/node-a/control", json={"load_fail": True})
        r = await client.post("/api/deployments", json={
            "model_name": "bad", "mem_per_replica_mb": 128, "replica_count": 1})
        await wait_for(client, r.json()["id"], {"FAILED"})
    reliability.evaluate_all()
    assert alert_of((await client.get("/api/reliability")).json(), "node-a")["status"] == "ALERT"

    old = reliability.iso(datetime.now(timezone.utc) - timedelta(seconds=300))
    with db.tx() as conn:
        conn.execute("UPDATE load_samples SET dispatched_at=?, deadline_at=? WHERE node_id=?",
                     (old, old, "node-a"))
    await client.post("/api/nodes/node-a/control",
                      json={"load_fail": False, "mode": "normal"})
    for i in range(2):  # 只有 2 条新样本，低于最低 3 条
        r = await client.post("/api/deployments", json={
            "model_name": f"ok{i}", "mem_per_replica_mb": 128, "replica_count": 2})
        await wait_for(client, r.json()["id"], {"AVAILABLE"})
    reliability.evaluate_all()
    a = alert_of((await client.get("/api/reliability")).json(), "node-a")
    assert a["status"] == "ALERT"
    assert "不足最低样本" in a["detail"]


@pytest.mark.asyncio
async def test_07_statistics_do_not_change_memory_or_deployment_state(client):
    """可靠性评估/告警不得改动显存预留与部署状态。"""
    set_thresholds("node-a", min_samples=1, ratio=0.5)
    r = await client.post("/api/deployments", json={
        "model_name": "hold", "mem_per_replica_mb": 1024, "replica_count": 2})
    dep_id = r.json()["id"]
    await wait_for(client, dep_id, {"AVAILABLE"})
    before_book = (await client.get("/api/nodes")).json()
    before_dep = (await client.get(f"/api/deployments/{dep_id}")).json()

    # 反复评估 + 用极端阈值也不会改变任何账本/部署
    for ratio in (0, 0.1, 1):
        set_thresholds("node-a", min_samples=1, ratio=ratio)
        reliability.evaluate_all()
    after_book = (await client.get("/api/nodes")).json()
    after_dep = (await client.get(f"/api/deployments/{dep_id}")).json()
    assert [(n["node_id"], n["used_mb"], n["free_mb"]) for n in before_book] == \
           [(n["node_id"], n["used_mb"], n["free_mb"]) for n in after_book]
    assert before_dep["status"] == after_dep["status"] == "AVAILABLE"
    assert [(x["id"], x["status"]) for x in before_dep["replicas"]] == \
           [(x["id"], x["status"]) for x in after_dep["replicas"]]


@pytest.mark.asyncio
async def test_09_ratio_equal_to_threshold_fires(client):
    """异常比例恰好等于阈值时触发（达到即告警），低于才正常。"""
    set_thresholds("node-a", window=300, min_samples=2, ratio=0.5)
    # 1 条失败 + 1 条按时 -> 50%，达到阈值
    await client.post("/api/nodes/node-a/control", json={"load_fail": True})
    r = await client.post("/api/deployments", json={
        "model_name": "mixed-bad", "mem_per_replica_mb": 64, "replica_count": 1})
    await wait_for(client, r.json()["id"], {"FAILED"})
    await client.post("/api/nodes/node-a/control",
                      json={"load_fail": False, "mode": "normal"})
    r = await client.post("/api/deployments", json={
        "model_name": "mixed-ok", "mem_per_replica_mb": 64, "replica_count": 2})
    await wait_for(client, r.json()["id"], {"AVAILABLE"})
    a = alert_of((await client.get("/api/reliability")).json(), "node-a")
    assert (a["on_time_count"], a["failed_count"], a["overdue_count"]) == (1, 1, 0)
    assert a["abnormal_ratio"] == 0.5
    assert a["status"] == "ALERT"

    # 阈值改到 0.6：同样样本低于阈值 -> 恢复
    set_thresholds("node-a", window=300, min_samples=2, ratio=0.6)
    a = alert_of((await client.get("/api/reliability")).json(), "node-a")
    assert a["status"] == "OK"


@pytest.mark.asyncio
async def test_08_node_loss_and_platform_restart_keep_samples_and_alert(client):
    """节点失联与平台重启后，样本依据与告警一致。"""
    set_thresholds("node-a", window=300, min_samples=2, ratio=0.5)
    await client.post("/api/nodes/node-a/control", json={"mode": "unreachable"})
    await service.health_monitor_once()
    # 双副本部署保证每轮都在 node-a 上留下一条逾期样本
    for _ in range(2):
        r = await client.post("/api/deployments", json={
            "model_name": "lost", "mem_per_replica_mb": 128, "replica_count": 2})
        await wait_for(client, r.json()["id"], {"UNVERIFIED"}, timeout=15)
    # 前一轮的吊销卸载可能因节点失联挂起到本轮才超时（一次加载下发只记一条样本），
    # 但它确实发生过、且为逾期；等它落定后再取样本快照。
    await wait_until(lambda: asyncio.sleep(0, reliability.evaluate_all()) or
                     len([s for s in samples_for("node-a") if s["status"] == "OVERDUE"]) >= 2)
    reliability.evaluate_all()
    body = (await client.get("/api/reliability")).json()
    a = alert_of(body, "node-a")
    assert a["status"] == "ALERT" and a["overdue_count"] == 2
    samples_before = [(s["replica_id"], s["epoch"], s["status"], s["dispatched_at"])
                      for s in samples_for("node-a")]
    book_before = [(n["node_id"], n["used_mb"]) for n in (await client.get("/api/nodes")).json()]

    # 模拟平台重启：丢弃内存连接与节点在线状态，重新打开同一 SQLite 库
    await service.node_client.close()
    from app import db as db_mod
    db_mod._conn = None
    service.init_db()
    service.NODE_RUNTIME.clear()
    for n in config.NODES:
        service.NODE_RUNTIME[n.node_id] = {"online": None, "last_check": None,
                                           "detail": "尚未探测", "mode": ""}
    reliability.evaluate_all()

    samples_after = [(s["replica_id"], s["epoch"], s["status"], s["dispatched_at"])
                     for s in samples_for("node-a")]
    assert samples_after == samples_before
    a2 = alert_of((await client.get("/api/reliability")).json(), "node-a")
    assert a2["status"] == "ALERT"
    assert (a2["on_time_count"], a2["failed_count"], a2["overdue_count"]) == \
           (a["on_time_count"], a["failed_count"], a["overdue_count"])
    book_after = [(n["node_id"], n["used_mb"]) for n in (await client.get("/api/nodes")).json()]
    assert book_after == book_before  # 重启不改变显存账本
