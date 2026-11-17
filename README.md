# 共享诊断 SQL 服务（FastAPI + PostgreSQL）

值班数据库工程师共用的诊断执行服务：提交至多 10 条可信 SQL，在**固定复用的两条
PostgreSQL 连接**之一上按序自动提交执行；支持逐句结果查询、取消未结束的单；
每单结束后清理会话状态（SET、临时表、会话级咨询锁等），清理失败则淘汰连接并补建。

## 架构

```
                ┌────────────── FastAPI (uvicorn, 单进程) ──────────────┐
  POST /jobs ──▶│  校验(sqlguard) → JobStore(内存注册表) → 共享队列       │
  GET  /jobs/*  │                                                        │
  POST ../cancel│  cancel: 置标志 + 202 即时受理 ──▶ CancelSender 线程    │
  GET  /pool    │       (可配置发送停顿) ──▶ conn.cancel()               │
                │                                                        │
                │  Worker 线程 #1 ── 连接 #1 (autocommit, 长连接)         │
                │  Worker 线程 #2 ── 连接 #2 (autocommit, 长连接)         │
                └────────────────────────┬───────────────────────────────┘
                                         ▼
                                   PostgreSQL
```

- **固定两条连接**：两个 worker 线程各自持有一条长连接（`autocommit=True`），
  从同一个队列取单。连接在启动时建立并反复复用，**绝不每单重连**；唯一的重建
  路径是"清理失败/连接损坏 → 淘汰旧连接 → 原地补建"（`generation` 递增，
  池子仍是两条）。
- **排队**：两条连接都忙时，新单在内存队列等待（默认上限 1000，超出返回 429）。
- **互不阻塞**：每条连接一个独立 worker；慢 SQL、慢清理只占用本连接的 worker，
  另一连接的执行、提交以及所有状态查询（读内存注册表）都不受影响。

## 快速开始

```bash
docker compose up --build
# API: http://localhost:8000  文档: http://localhost:8000/docs
```

本地开发（需自有 PostgreSQL）：

```bash
pip install -r requirements.txt
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diag \
  uvicorn app.main:app --port 8000
```

演示脚本（覆盖：顺序执行、会话清理验证、取消、慢取消发送/慢清理钩子、迟到取消）：

```bash
./scripts/demo.sh            # BASE=http://localhost:8000 可覆盖
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/jobs` | 提交 1–10 条 SQL（202，返回 `job_id`） |
| GET | `/jobs` | 最近的单（摘要列表） |
| GET | `/jobs/{id}` | 单详情 + 逐句结果（状态/列/行/rowcount/耗时/错误） |
| POST | `/jobs/{id}/cancel` | 取消未结束的单（202 受理；已结束返回 409） |
| GET | `/pool` | 两条连接的状态（idle/busy/cleaning/reconnecting、代数、清理失败数） |
| GET | `/replication-slots/{name}/lag` | 按槽名两次采样 WAL 滞留与变化（独立短连接，不占用诊断池） |
| GET | `/health` | 存活检查 |
| GET | `/docs` | Swagger UI |

### 复制槽 WAL 滞留采样

`GET /replication-slots/{slot_name}/lag?interval=2`（`interval` 限定 1～3 秒，默认 2 秒）。
在 `interval` 间隔取两次样本，每次读取当前 WAL 位置 `pg_current_wal_lsn()` 与该槽的
`restart_lsn` / `wal_status` / `safe_wal_size`，返回：

- 两次原始样本（LSN 以 `X/XXXXXXXX` 文本原样返回；`restart_lsn` 为 NULL 时保留 `null`，
  **绝不当作 0**）；
- 滞留字节 `current_wal_lsn - restart_lsn` 两次取值与净变化 `delta_retained_bytes`，
  以及当前位置、restart_lsn 各自前进的字节数；
- 仅在**全部条件**满足时才给出剩余时间估算：两次样本可比较、滞留**净增**（`delta > 0`）、
  且存在有限保留限额（`safe_wal_size` 非 NULL）。估算 = `safe_wal_size / 净增速`，并显式
  标注 `short_window_extrapolation`（仅基于这一短暂窗口的线性外推，属指示性倒计时而非保证）。

下列情况返回 `status=not_estimable` 并给出机器可读 `reason`，不产生倒计时：

| reason | 触发条件 |
|---|---|
| `slot_not_found` | 任一采样中槽不存在 |
| `restart_lsn_null` | `restart_lsn` 为 NULL（槽从未预留 WAL，NULL 不当零） |
| `wal_status_unreserved` / `wal_status_lost` | 所需 WAL 已不被保留 |
| `current_lsn_moved_backward` / `restart_lsn_moved_backward` | 采样期间位置异常回退（不可比较） |
| `slot_identity_changed` / `malformed_lsn` | 槽属性变化或 LSN 形态异常 |
| `no_net_growth` | 滞留净变化 ≤ 0（消费端在跟上/追上来，滞留持平或下降，不报倒计时） |
| `no_retention_limit` | `safe_wal_size` 为 NULL（`max_slot_wal_keep_size=-1`，无有限限额可倒计时） |

**连接策略**：采样在请求自己的**短生命周期连接**上完成（本请求内打开、响应前关闭），
不借用池中两条固定诊断长连接——因此两条诊断连接被慢任务占满时，采样照常快速返回，
任务提交与取消也始终可用（见 `test_sampling_does_not_consume_pool_connections`）。

### 提交

```bash
curl -XPOST localhost:8000/jobs -H 'content-type: application/json' -d '{
  "statements": ["SET work_mem = '"'"'64MB'"'"'", "SELECT count(*) FROM pg_stat_activity"],
  "options": {"cleanup_delay_ms": 0, "break_connection_after": false}
}'
```

- 每条元素必须是**一条**语句（`;` 分隔的多句会被 422 拒绝；字符串/美元引号/
  注释里的 `;` 不算）。
- **拒绝显式事务控制**：`BEGIN/START/COMMIT/ROLLBACK/ABORT/END/SAVEPOINT/
  RELEASE`、`SET TRANSACTION`、`SET SESSION CHARACTERISTICS`、
  `PREPARE TRANSACTION`（含注释伪装的写法），返回 422。每条语句各自自动提交。
- 单语句错误 → 该句记 `error`，后续句记 `skipped`，单状态 `failed`。

### 取消语义

- **及时受理**：`POST /jobs/{id}/cancel` 立即置标志并返回 202；真正的取消发送
  由独立线程完成（可用 `{"delay_ms": N}` 模拟慢取消管道），不阻塞受理。
- **受理后**：正在执行的语句被 `conn.cancel()` 中断（PostgreSQL 标准取消），
  后续语句全部 `skipped`；**此前已提交的语句效果保留**（自动提交，无回滚）。
  极端时序下已发往服务器的语句可能自然完成，属正常现象。
- **排队中的单**：直接标记 `cancelled`，不会执行。
- **迟到取消**：单已结束时取消返回 409（无操作）；发送线程在发送前按连接校验
  "该单仍是此连接当前单且未结束"，因此**迟到的取消绝不会误停同一连接上的下一单**。
- **连接归还条件**：SQL 停止 + 取消发送完成 + 会话清理完成，三者齐备后连接才
  回到空闲（可用延迟钩子直接观测）。

### 会话清理（每单结束后，成功/失败/取消都执行）

`RESET ALL`、`RESET ROLE`、`RESET SESSION AUTHORIZATION`、`DEALLOCATE ALL`、
`DISCARD SEQUENCES`、删除本会话全部临时表/视图/序列、
`pg_advisory_unlock_all()`。清理失败 → 关闭并淘汰该连接，原地补建新连接
（`/pool` 可见 `generation` 与 `cleanups_failed` 递增）。

## 测试钩子（可控停顿/故障注入）

| 钩子 | 位置 | 作用 |
|---|---|---|
| `options.cleanup_delay_ms` | 提交单时 | 清理阶段停顿 N 毫秒（模拟慢清理） |
| `{"delay_ms": N}` | 取消请求体 | 受理后延迟 N 毫秒再向 PG 发送取消（模拟慢取消发送） |
| `options.break_connection_after` | 提交单时 | 单结束后断开本连接（模拟连接故障），触发淘汰+补建 |
| `CANCEL_SEND_DELAY_MS` / `CLEANUP_DELAY_MS` | 环境变量 | 上述两个延迟的全局默认值 |

## 自测

```bash
pip install -r requirements-dev.txt
pytest tests/test_sqlguard.py        # 纯单元测试（SQL 守卫），无需数据库
pytest tests/test_slotsampler.py     # 纯单元测试（滞留采样判定/外推逻辑），无需数据库
# 集成测试需要运行中的服务与数据库：
API_BASE=http://localhost:8000 TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diag \
  pytest tests/test_integration.py tests/test_slot_lag_integration.py
```

集成测试覆盖：按序自动提交与逐句结果、行数截断、失败即停、参数校验、
会话状态不泄漏（SET/临时表/咨询锁/预备语句，含失败单）、取消运行中/排队中的单、
迟到取消（409 与延迟发送不误伤下一单）、慢 SQL/慢清理不阻塞另一连接、
清理失败淘汰并补建连接（钩子注入 + `pg_terminate_backend` 真实杀连接两种）。

复制槽采样集成测试（`test_slot_lag_integration.py`）覆盖：槽缺失、`interval`
越界 422、`restart_lsn` 为 NULL（保留 null 不当零）、净增 + 有限限额给出标注为
短时外推的倒计时、无限额（`safe_wal_size` NULL）不报倒计时、静止槽零增长不报倒计时、
消费端推进使滞留下降/持平时不报倒计时，以及**两条诊断连接被占满时采样仍即时返回、
池子始终保持两条连接、取消不受影响**。

> 注：PostgreSQL 的语句取消依赖向后端进程投递 SIGINT。在禁止进程间信号的
> 沙箱里，"中断正在执行的语句"无法生效（`pg_terminate_backend` 同样无效），
> 相关测试用例设计为在两种环境下都通过：沙箱内验证协作式停止（语句间停止），
> Docker/正常 Linux 下同一代码路径会真正中断执行中的语句。

## 设计说明与限制

- 面向**可信 SQL**（值班 DBA）：守卫只强制执行契约（单句、禁显式事务），
  不是安全边界；`CALL`/`DO` 体内的事务控制无法静态识别，属约定范围。
- 任务注册表在内存中（重启即失），保留最近 1000 单。
- 结果行数上限 `ROW_LIMIT`（默认 200，超出截断并标记 `truncated`）。
- 单进程单实例：连接池在进程内，勿以多 worker 方式启动 uvicorn。
- 连接使用 `prepare_threshold=None`，避免 psycopg 的客户端预备语句缓存与
  清理中的 `DEALLOCATE ALL` 冲突。
