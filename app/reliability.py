"""加载可靠性采样与按节点告警。

语义：
- 每次“加载下发”生成一条样本（replica_id + epoch 唯一），下发之外的轮询、
  手动对账、迟到回执都只更新同一条样本，绝不新增样本；
- 样本判定后冻结：ON_TIME / FAILED / OVERDUE 不会被后续回执改写。
  超过加载时限仍未确认即为 OVERDUE，之后即使对账确认加载成功也仍是 OVERDUE；
- 失败重试会创建新的副本（新 epoch / 新 replica_id），因此另算一条样本；
- 告警按节点统计观察窗内的样本：有效样本数达到最低样本数且异常比例
  （失败 + 逾期）/ 有效样本数 ≥ 阈值时触发；
- 只有“新近有效样本数 ≥ 最低样本数且异常比例 < 阈值”才恢复。
  窗口内样本不足（包含窗口为空）一律保持原状态，不能算恢复；
- 本模块只写 load_samples / node_alerts / node_alert_settings / events，
  绝不修改 replicas / deployments，因此不影响显存预留与部署状态。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from . import config, db, models


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds")


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


DEFAULT_SETTINGS: dict[str, Any] = {
    "window_seconds": config.ALERT_WINDOW_SECONDS,
    "min_samples": config.ALERT_MIN_SAMPLES,
    "abnormal_ratio": config.ALERT_ABNORMAL_RATIO,
}


# ---------------------------------------------------------------- 样本写入
def record_dispatch(conn, rep) -> None:
    """记录一次加载下发样本。同一副本同一 epoch 只会有一条（幂等）。"""
    dispatched = utcnow()
    deadline = dispatched + timedelta(seconds=config.LOAD_TIMEOUT)
    conn.execute(
        "INSERT OR IGNORE INTO load_samples(replica_id,deployment_id,epoch,node_id,"
        "dispatched_at,deadline_at,status,decided_at) VALUES(?,?,?,?,?,?,?,NULL)",
        (rep["id"], rep["deployment_id"], rep["epoch"], rep["node_id"],
         iso(dispatched), iso(deadline), models.SAMPLE_PENDING),
    )


def _settle(conn, replica_id: str, epoch: int, status: str) -> bool:
    """仅把仍为 PENDING 的样本判定为 status；已判定样本保持不变。"""
    cur = conn.execute(
        "UPDATE load_samples SET status=?,decided_at=? "
        "WHERE replica_id=? AND epoch=? AND status=?",
        (status, iso(utcnow()), replica_id, epoch, models.SAMPLE_PENDING),
    )
    return cur.rowcount > 0


def settle_confirmation(conn, replica_id: str, epoch: int) -> None:
    """节点确认加载完成：截止前 -> 按时；截止后 -> 逾期（补确认不改判）。"""
    row = conn.execute(
        "SELECT * FROM load_samples WHERE replica_id=? AND epoch=?", (replica_id, epoch)
    ).fetchone()
    if not row or row["status"] != models.SAMPLE_PENDING:
        return
    status = (models.SAMPLE_ON_TIME if utcnow() <= _parse(row["deadline_at"])
              else models.SAMPLE_OVERDUE)
    _settle(conn, replica_id, epoch, status)


def settle_failed(conn, replica_id: str, epoch: int) -> None:
    """加载明确失败（节点显式拒绝/上报失败/确认无此副本）。"""
    _settle(conn, replica_id, epoch, models.SAMPLE_FAILED)


def settle_overdue(conn, replica_id: str, epoch: int) -> None:
    """加载超时/调用失联：本次下发未能按时确认，记为逾期。"""
    _settle(conn, replica_id, epoch, models.SAMPLE_OVERDUE)


def mark_due_samples(conn, at: datetime | None = None) -> int:
    """把已过截止时间仍等待确认的样本判为逾期。只改样本，不碰副本状态。"""
    at = at or utcnow()
    rows = conn.execute(
        "SELECT * FROM load_samples WHERE status=?", (models.SAMPLE_PENDING,)
    ).fetchall()
    changed = 0
    for row in rows:
        if _parse(row["deadline_at"]) > at:
            continue
        if _settle(conn, row["replica_id"], row["epoch"], models.SAMPLE_OVERDUE):
            changed += 1
            conn.execute(
                "INSERT INTO events(deployment_id,replica_id,node_id,event_type,detail,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (row["deployment_id"], row["replica_id"], row["node_id"],
                 "LOAD_SAMPLE_OVERDUE",
                 f"加载超过 {config.LOAD_TIMEOUT:.0f}s 未确认，本次样本记为逾期"
                 "（之后补确认成功也不改判）", iso(at)),
            )
    return changed


# ---------------------------------------------------------------- 阈值配置
def get_settings(conn, node_id: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM node_alert_settings WHERE node_id=?", (node_id,)
    ).fetchone()
    if not row:
        return dict(DEFAULT_SETTINGS)
    return {
        "window_seconds": row["window_seconds"],
        "min_samples": row["min_samples"],
        "abnormal_ratio": row["abnormal_ratio"],
    }


def validate_settings(window_seconds: float, min_samples: int,
                      abnormal_ratio: float) -> dict[str, Any]:
    if not (1 <= window_seconds <= 86400):
        raise ValueError("观察窗需在 1～86400 秒之间")
    if not (1 <= min_samples <= 10000):
        raise ValueError("最低样本数需在 1～10000 之间")
    if not (0 <= abnormal_ratio <= 1):
        raise ValueError("异常比例阈值需在 0～1 之间")
    return {
        "window_seconds": float(window_seconds),
        "min_samples": int(min_samples),
        "abnormal_ratio": float(abnormal_ratio),
    }


def save_settings(conn, node_id: str, cfg: dict[str, Any]) -> None:
    conn.execute(
        "INSERT INTO node_alert_settings(node_id,window_seconds,min_samples,abnormal_ratio,updated_at) "
        "VALUES(?,?,?,?,?) ON CONFLICT(node_id) DO UPDATE SET "
        "window_seconds=excluded.window_seconds,min_samples=excluded.min_samples,"
        "abnormal_ratio=excluded.abnormal_ratio,updated_at=excluded.updated_at",
        (node_id, cfg["window_seconds"], cfg["min_samples"], cfg["abnormal_ratio"], iso(utcnow())),
    )


def ensure_settings(conn, node_id: str) -> None:
    """节点首次出现时落一条默认阈值，之后网页/API 修改的值会持久保留。"""
    conn.execute(
        "INSERT OR IGNORE INTO node_alert_settings(node_id,window_seconds,min_samples,"
        "abnormal_ratio,updated_at) VALUES(?,?,?,?,?)",
        (node_id, DEFAULT_SETTINGS["window_seconds"], DEFAULT_SETTINGS["min_samples"],
         DEFAULT_SETTINGS["abnormal_ratio"], iso(utcnow())),
    )


# ---------------------------------------------------------------- 告警判定
def _counts(conn, node_id: str, window_seconds: float, at: datetime) -> dict[str, int]:
    start = at - timedelta(seconds=window_seconds)
    rows = conn.execute(
        "SELECT status, COUNT(*) AS n FROM load_samples "
        "WHERE node_id=? AND dispatched_at>=? GROUP BY status",
        (node_id, iso(start)),
    ).fetchall()
    counts = {s: 0 for s in (models.SAMPLE_PENDING, models.SAMPLE_ON_TIME,
                             models.SAMPLE_FAILED, models.SAMPLE_OVERDUE)}
    for r in rows:
        counts[r["status"]] = r["n"]
    return counts


def evaluate_node(conn, node_id: str, at: datetime | None = None) -> dict[str, Any]:
    at = at or utcnow()
    cfg = get_settings(conn, node_id)
    counts = _counts(conn, node_id, cfg["window_seconds"], at)
    on_time = counts[models.SAMPLE_ON_TIME]
    failed = counts[models.SAMPLE_FAILED]
    overdue = counts[models.SAMPLE_OVERDUE]
    pending = counts[models.SAMPLE_PENDING]
    resolved = on_time + failed + overdue
    abnormal = failed + overdue
    ratio = abnormal / resolved if resolved else None

    prev = conn.execute(
        "SELECT * FROM node_alerts WHERE node_id=?", (node_id,)
    ).fetchone()
    prev_status = prev["status"] if prev else models.ALERT_OK

    enough = resolved >= cfg["min_samples"]
    threshold = cfg["abnormal_ratio"]
    ratio_text = f"{ratio:.0%}" if ratio is not None else "—"

    if prev_status == models.ALERT_FIRING:
        if enough and ratio is not None and ratio < threshold:
            status = models.ALERT_OK
            detail = (f"新近 {resolved} 个有效样本异常率 {ratio_text} 低于阈值 "
                      f"{threshold:.0%}，恢复")
            recovered_at = iso(at)
        else:
            status = models.ALERT_FIRING
            recovered_at = prev["recovered_at"]
            if not enough:
                detail = (f"观察窗内有效样本 {resolved} 个（等待确认 {pending} 个），"
                          f"不足最低样本 {cfg['min_samples']}，窗口为空或样本不足不能算恢复，"
                          f"维持告警")
            else:
                detail = (f"观察窗内 {resolved} 个有效样本异常 {abnormal} 个"
                          f"（失败 {failed}、逾期 {overdue}），异常率 {ratio_text} "
                          f"未低于阈值 {threshold:.0%}，维持告警")
    else:
        if enough and ratio is not None and ratio >= threshold:
            status = models.ALERT_FIRING
            detail = (f"观察窗内 {resolved} 个有效样本异常 {abnormal} 个"
                      f"（失败 {failed}、逾期 {overdue}），异常率 {ratio_text} "
                      f"达到阈值 {threshold:.0%}，触发告警")
            recovered_at = prev["recovered_at"] if prev else None
        else:
            status = models.ALERT_OK
            recovered_at = prev["recovered_at"] if prev else None
            if not enough:
                detail = (f"观察窗内有效样本 {resolved} 个（等待确认 {pending} 个），"
                          f"不足最低样本 {cfg['min_samples']}，未触发")
            else:
                detail = (f"观察窗内 {resolved} 个有效样本异常率 {ratio_text} "
                          f"低于阈值 {threshold:.0%}，正常")

    if status == models.ALERT_FIRING:
        firing_since = prev["firing_since"] if prev and prev["status"] == models.ALERT_FIRING else iso(at)
    else:
        firing_since = None if prev is None or prev["status"] != models.ALERT_FIRING else prev["firing_since"]

    changed = prev is None or prev["status"] != status
    conn.execute(
        "INSERT INTO node_alerts(node_id,status,firing_since,recovered_at,last_evaluated_at,"
        "window_seconds,min_samples,abnormal_ratio,sample_count,on_time_count,failed_count,"
        "overdue_count,pending_count,abnormal_ratio_actual,detail) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(node_id) DO UPDATE SET status=excluded.status,"
        "firing_since=excluded.firing_since,recovered_at=excluded.recovered_at,"
        "last_evaluated_at=excluded.last_evaluated_at,window_seconds=excluded.window_seconds,"
        "min_samples=excluded.min_samples,abnormal_ratio=excluded.abnormal_ratio,"
        "sample_count=excluded.sample_count,on_time_count=excluded.on_time_count,"
        "failed_count=excluded.failed_count,overdue_count=excluded.overdue_count,"
        "pending_count=excluded.pending_count,abnormal_ratio_actual=excluded.abnormal_ratio_actual,"
        "detail=excluded.detail",
        (node_id, status, firing_since, recovered_at, iso(at),
         cfg["window_seconds"], cfg["min_samples"], cfg["abnormal_ratio"],
         resolved, on_time, failed, overdue, pending, ratio, detail),
    )
    if changed:
        conn.execute(
            "INSERT INTO events(deployment_id,replica_id,node_id,event_type,detail,created_at) "
            "VALUES(NULL,NULL,?,?,?,?)",
            (node_id,
             "ALERT_FIRING" if status == models.ALERT_FIRING else "ALERT_RECOVERED",
             detail, iso(at)),
        )
    return alert_dict(conn, node_id, at)


def alert_dict(conn, node_id: str, at: datetime | None = None) -> dict[str, Any]:
    at = at or utcnow()
    row = conn.execute("SELECT * FROM node_alerts WHERE node_id=?", (node_id,)).fetchone()
    cfg = get_settings(conn, node_id)
    if not row:
        counts = _counts(conn, node_id, cfg["window_seconds"], at)
        resolved = counts[models.SAMPLE_ON_TIME] + counts[models.SAMPLE_FAILED] + counts[models.SAMPLE_OVERDUE]
        return {
            "status": models.ALERT_OK, "status_cn": models.ALERT_STATUS_CN[models.ALERT_OK],
            "firing_since": None, "recovered_at": None, "last_evaluated_at": None,
            "config": cfg, "sample_count": resolved,
            "on_time_count": counts[models.SAMPLE_ON_TIME],
            "failed_count": counts[models.SAMPLE_FAILED],
            "overdue_count": counts[models.SAMPLE_OVERDUE],
            "pending_count": counts[models.SAMPLE_PENDING],
            "abnormal_ratio": None, "detail": "尚未评估",
        }
    return {
        "status": row["status"],
        "status_cn": models.ALERT_STATUS_CN.get(row["status"], row["status"]),
        "firing_since": row["firing_since"], "recovered_at": row["recovered_at"],
        "last_evaluated_at": row["last_evaluated_at"],
        "config": {
            "window_seconds": row["window_seconds"],
            "min_samples": row["min_samples"],
            "abnormal_ratio": row["abnormal_ratio"],
        },
        "sample_count": row["sample_count"],
        "on_time_count": row["on_time_count"],
        "failed_count": row["failed_count"],
        "overdue_count": row["overdue_count"],
        "pending_count": row["pending_count"],
        "abnormal_ratio": row["abnormal_ratio_actual"],
        "detail": row["detail"],
    }


def evaluate_all() -> list[dict[str, Any]]:
    """一次评估：先冻结所有到期未确认样本为逾期，再逐节点判定告警。"""
    at = utcnow()
    with db.tx() as conn:
        mark_due_samples(conn, at)
        return [evaluate_node(conn, n.node_id, at) for n in config.NODES]


def set_settings(node_id: str, window_seconds: float, min_samples: int,
                 abnormal_ratio: float) -> dict[str, Any]:
    cfg = validate_settings(window_seconds, min_samples, abnormal_ratio)
    with db.tx() as conn:
        save_settings(conn, node_id, cfg)
    evaluate_all()
    return reliability_state(node_id)


# ---------------------------------------------------------------- 查询
def _samples(conn, node_id: str, window_seconds: float, at: datetime,
             limit: int = 50) -> list[dict[str, Any]]:
    start = at - timedelta(seconds=window_seconds)
    rows = conn.execute(
        "SELECT s.*, d.model_name FROM load_samples s "
        "LEFT JOIN deployments d ON d.id=s.deployment_id "
        "WHERE s.node_id=? AND s.dispatched_at>=? ORDER BY s.id DESC LIMIT ?",
        (node_id, iso(start), limit),
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["status_cn"] = models.SAMPLE_STATUS_CN.get(r["status"], r["status"])
        item["round"] = r["epoch"]
        out.append(item)
    return out


def _abnormal_records(conn, node_id: str, window_seconds: float,
                      at: datetime) -> list[dict[str, Any]]:
    """观察窗内被判为失败/逾期的样本明细（副本 + 轮次），供告警列示。"""
    start = at - timedelta(seconds=window_seconds)
    rows = conn.execute(
        "SELECT s.replica_id, s.deployment_id, s.epoch, s.status, s.dispatched_at, "
        "s.decided_at, d.model_name FROM load_samples s "
        "LEFT JOIN deployments d ON d.id=s.deployment_id "
        "WHERE s.node_id=? AND s.dispatched_at>=? AND s.status IN (?,?) "
        "ORDER BY s.id DESC",
        (node_id, iso(start), models.SAMPLE_FAILED, models.SAMPLE_OVERDUE),
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["round"] = r["epoch"]
        item["status_cn"] = models.SAMPLE_STATUS_CN.get(r["status"], r["status"])
        out.append(item)
    return out


def reliability_state(node_id: str | None = None) -> dict[str, Any]:
    at = utcnow()
    nodes = db.query("SELECT * FROM nodes ORDER BY node_id")
    if node_id is not None:
        nodes = [n for n in nodes if n["node_id"] == node_id]
    out_nodes = []
    for n in nodes:
        cfg = get_settings(db.connect(), n["node_id"])
        out_nodes.append({
            "node_id": n["node_id"], "name": n["name"],
            "config": cfg,
            "alert": alert_dict(db.connect(), n["node_id"], at),
            "abnormal_samples": _abnormal_records(
                db.connect(), n["node_id"], cfg["window_seconds"], at),
            "samples": _samples(db.connect(), n["node_id"], cfg["window_seconds"], at),
        })
    result: dict[str, Any] = {
        "now": iso(at),
        "load_timeout_seconds": config.LOAD_TIMEOUT,
        "nodes": out_nodes,
    }
    if node_id is not None:
        if not out_nodes:
            return {}
        return out_nodes[0] | {"load_timeout_seconds": config.LOAD_TIMEOUT, "now": result["now"]}
    return result
