# 推理平台副本调度值班台

值班员用于在两台共享推理节点上安排模型副本，核心目标是**避免加载超时后重复占用显存**。
FastAPI + SQLite 实现，Docker Compose 一键启动 1 个调度平台 + 2 台可注入故障的本地节点。

## 一、快速启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

| 服务 | 端口 | 说明 |
| --- | --- | --- |
| platform | 8000 | 调度平台（网页 + REST API + 后台轮询/健康监测） |
| node-a | 9101 | 模拟节点 A，固定显存 8192MB |
| node-b | 9102 | 模拟节点 B，固定显存 8192MB |

不使用 Docker 时的本地运行方式：

```bash
pip install -r requirements.txt
# 终端 1/2：两个节点
NODE_ID=node-a NODE_NAME=模拟节点A CAPACITY_MB=8192 \
  uvicorn node.simulator:app --port 9101
NODE_ID=node-b NODE_NAME=模拟节点B CAPACITY_MB=8192 \
  uvicorn node.simulator:app --port 9102
# 终端 3：调度平台
PLATFORM_NODES="node-a|模拟节点A|http://127.0.0.1:9101|8192,node-b|模拟节点B|http://127.0.0.1:9102|8192" \
  uvicorn app.main:app --port 8000
```

## 二、网页操作

- **提交部署**：模型名、每副本显存（MB）、副本数（1～2）。
- **节点账本**：实时显示每台节点容量、已预留、剩余显存与在线状态。
- **部署列表**：每个部署的副本分配（节点、epoch、状态、错误原因）与事件流水。
- **故障注入**：对每台节点一键切换 正常 / 延迟 / 失败(503) / 失联(挂起超时)，
  以及“下次加载失败”“迟到回执”“重置节点”。
- **加载可靠性告警**：按节点展示观察窗内样本（按时确认/失败/逾期）、异常率、
  告警状态与对应副本/轮次；可现场修改观察窗、最低样本数、异常比例阈值，
  并有“模拟慢加载”按钮一键复现。
- **手动对账 / 重试**：部署行内按钮；页面 2 秒自动刷新。

## 三、核心调度规则（与需求逐条对应）

1. **副本分散**：同一部署的 2 个副本必须落在不同节点（按剩余显存降序选节点）。
2. **全部确认才可用**：所有副本都被节点确认 `loaded` 后部署才标记 `AVAILABLE`。
3. **并发不超额预留**：预留检查与插入在同一个 SQLite 写事务内串行执行，
   显存账本只统计仍占用的副本（`PENDING/LOADING/LOADED/TIMEOUT/UNVERIFIED/RELEASING`），
   不足时整个部署返回 409，不会产生半分配。
4. **超时/失联保留占用待核**：
   - 传输层错误（超时、连接失败）→ 节点真实状态不可知 → 副本转 `TIMEOUT`，**显存继续占用**；
   - 显式 HTTP 失败（4xx/5xx）→ 节点确认未占用 → 副本转 `LOAD_FAILED` 并释放预留。
5. **恢复对账**：节点恢复后由健康监测或业务调用成功联动触发自动对账，也可手动对账：
   - 节点无残留 → 副本确认 `RELEASED`，显存才可重新分配；
   - 节点其实已加载 → 同一 epoch 内补确认（合法，不算复活）；
   - 节点仍持有旧加载 → 重新下发卸载，直到确认释放；
   - 对账时节点仍失联 → 保持待核并在原因/备注中说明，绝不擅自释放。
6. **迟到回执不复活旧副本**：副本带 `epoch` 标识；重试开启新 epoch，
   节点侧与调度器侧都只接受当前 epoch 的状态，旧 epoch 回执被丢弃。
7. **失败可重试**：仅 `FAILED` 部署允许重试，重试前校验旧副本全部不再占显存，
   旧副本记录保留为终态用于审计。

## 四、加载可靠性告警（按节点）

值班场景：节点加载链路持续变慢时，需要在不影响调度账本的前提下独立发现并告警；
部署后来恢复可用，也不能抹去先前的逾期记录。

**样本口径**

- **一次加载下发 = 一条样本**（`load_samples`，`replica_id + epoch` 唯一）。
  轮询、手动对账、健康探测、迟到回执都只更新这一条样本，**绝不新增样本**；
- 样本判定后**冻结**，后续回执不改写：
  - `ON_TIME` 按时确认（截止 = 下发时间 + `LOAD_TIMEOUT`）；
  - `FAILED` 截止前明确失败（显式 HTTP 拒绝、节点上报 failed、对账确认无此副本）；
  - `OVERDUE` 超过截止时间仍未确认。**加载逾期后即使对账补确认成功，
    该样本仍记为逾期**（副本状态可以因合法补确认转为 `LOADED`，样本不改判）；
- **失败重试的新轮次另算**：重试会创建新副本（新 epoch / 新 replica_id），
  因此新样本与旧样本互不影响；旧轮次保留原判定用于审计。

> 判定以平台“收到确认”的时刻为准：若平台在截止时间前未收到确认（含平台停机期间的
> 确认窗口），样本即按逾期计，不会因为事后补确认而回填为按时。这样重启前后的口径一致。

**告警规则**

- 观察窗 = 最近 `window_seconds` 内下发的样本；有效样本 = 已判定（按时+失败+逾期），
  仍在等待确认的不计入分母；
- **触发**：有效样本数 ≥ `min_samples` 且 异常比例 =（失败+逾期）/ 有效样本 ≥ `abnormal_ratio`；
- **恢复**：只有有效样本数仍 ≥ `min_samples` 且异常比例 < 阈值才恢复。
  窗口内样本不足（包括**窗口为空**）一律维持原状态，**不能算恢复**；
- 告警列示观察窗内失败/逾期的**副本、轮次（epoch）、模型与下发时间**。

**一致性**

- 样本、阈值、告警状态全部落 SQLite（`load_samples` / `node_alert_settings` / `node_alerts`），
  节点失联、平台重启后依据不变；评估周期独立于节点健康探测；
- 告警统计**只写**上述表和 `events`，绝不修改 `replicas` / `deployments`，
  因此不改变显存预留与部署状态（自测用例专门断言这一点）。

## 五、状态机

部署：`PENDING（调度中）→ AVAILABLE（可用）/ FAILED（失败可重试）/ UNVERIFIED（待核）`

副本：
`PENDING → LOADING → LOADED`；异常分支 `TIMEOUT/UNVERIFIED → RELEASING → RELEASED`，
显式失败为 `LOAD_FAILED`（终态，不占显存）。

## 六、REST API 摘要

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| POST | `/api/deployments` | 提交部署 `{model_name, mem_per_replica_mb, replica_count}` |
| GET | `/api/deployments` / `/api/deployments/{id}` | 部署列表/详情（含副本与事件） |
| POST | `/api/deployments/{id}/retry` | 失败部署重试（新 epoch） |
| POST | `/api/deployments/{id}/reconcile` | 手动对账，返回逐条处置说明 |
| GET | `/api/nodes` | 节点显存账本与在线状态 |
| POST | `/api/nodes/{id}/control` | 故障注入（mode/延迟/load_fail 等） |
| POST | `/api/nodes/{id}/reset` | 重置节点模拟器 |
| GET | `/api/reliability` | 按节点的告警状态、阈值、观察窗样本及失败/逾期副本轮次 |
| GET | `/api/nodes/{id}/reliability` | 单节点可靠性详情（含样本） |
| PUT | `/api/nodes/{id}/alert-settings` | 修改告警阈值 `{window_seconds,min_samples,abnormal_ratio}` |
| GET | `/api/events` | 最近事件流水 |

## 七、自测

```bash
pip install -r requirements.txt pytest pytest-asyncio
pytest -q
```

`tests/test_platform.py` 基于 ASGI 传输同时拉起调度器与两台独立节点，覆盖：
完整部署流程、并发超额预留、显式失败释放与重试、失联超时保留占用、
恢复对账撤销旧加载并确认释放、迟到回执不复活旧 epoch、对账失败保持待核并说明原因。

`tests/test_reliability.py` 覆盖加载可靠性告警：逾期样本冻结（对账补确认仍算逾期）、
轮询/手动对账/迟到回执不重复计数、失败重试新轮次另算、告警触发与
“窗口为空/样本不足不算恢复、足量正常样本才恢复”、统计不改变显存与部署状态、
节点失联与平台重启后样本和告警一致。

## 八、目录结构

```
app/            调度平台
  config.py     环境配置（节点、超时、轮询间隔）
  db.py         SQLite 访问层（WAL + 写锁事务）
  models.py     状态常量与中文映射
  node_client.py 节点 HTTP 客户端（区分传输失败与显式 HTTP 失败）
  service.py    调度核心：预留/加载/超时/对账/重试/健康监测
  reliability.py 加载可靠性采样、逾期冻结、按节点告警与阈值
  main.py       FastAPI 入口、后台任务、故障注入代理
  static/index.html 值班网页
node/
  simulator.py  节点模拟器（应用工厂，支持延迟/失败/失联/迟到回执）
tests/          端到端自测
docker-compose.yml / Dockerfile
```

## 九、关键环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PLATFORM_NODES` | 两台 8192MB 节点 | `id|名称|url|容量MB`，逗号分隔 |
| `PLATFORM_DB` | `data/platform.db` | SQLite 路径 |
| `NODE_CALL_TIMEOUT` | `1.5` 秒 | 单次节点 HTTP 调用超时 |
| `LOAD_TIMEOUT` | `30` 秒 | 等待加载确认上限，超时转待核 |
| `POLL_INTERVAL` / `HEALTH_INTERVAL` | `1` / `10` 秒 | 轮询与健康探测间隔 |
| `ALERT_WINDOW_SECONDS` | `300` 秒 | 告警观察窗默认值（网页可改） |
| `ALERT_MIN_SAMPLES` | `3` | 最低有效样本数默认值（网页可改） |
| `ALERT_ABNORMAL_RATIO` | `0.5` | 异常比例阈值默认值（网页可改） |
| `ALERT_EVAL_INTERVAL` | `2` 秒 | 逾期判定与告警评估周期 |
