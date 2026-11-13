# 多来源采集任务编排与运行治理平台

按单一职责拆分：

| 模块 | 职责 |
| --- | --- |
| `sources.py` | 读取任务定义（JSON fixture） |
| `models.py` | `TaskSpec` / `RunConfig` / `RunConfig` 校验，去重与默认值 |
| `scheduler.py` | 依赖图校验、优先级与公平排序、重试策略、并发/分组限额、事件循环 |
| `clock.py` | 可注入时钟（`RealClock` 生产、`ManualClock` 虚拟时间） |
| `executor.py` | 可注入本地异步执行器 `LocalExecutor` |
| `storage.py` | SQLite 运行存储：任务终态、每次尝试、完整事件轨迹、并发水位 |
| `pipeline.py` | 编排装配、配置加载、事件落库 |
| `application.py` / `cli.py` | 兼容入口与命令行 |

默认使用本地 fixture，不连接外部服务；只调度可控本地任务，不涉及外部门户提交或跨进程崩溃恢复。

## 任务定义（fixture）

原有字段保持不变；新字段均可选，因此**没有新字段的旧 fixture 仍可执行**：

```json
[
  {"task_id": "catalog-full", "priority": 10, "mode": "full", "duration": 6,
   "resource_group": "catalog"},
  {"task_id": "catalog-inc", "priority": 8, "mode": "incremental", "duration": 2,
   "requires": ["catalog-full"], "resource_group": "catalog"},
  {"task_id": "legacy-extract", "priority": 4, "mode": "full", "duration": 1,
   "fail_until": 99, "retry_backoff": 1, "resource_group": "legacy"}
]
```

- `requires`：前置任务列表；为空表示无前置。
- `resource_group`：来源资源组，默认 `default`。
- `duration`：本地执行占用时长（虚拟秒或真实秒，取决于时钟）。
- `retry_backoff`：覆盖运行级重试退避基数。
- `fail_until`：前 N 次尝试失败（沿用原语义）。

## 运行配置

任务 fixture 保持原结构，限额放在独立的可选配置文件（`--config`）：

```json
{
  "max_parallel": 3,
  "group_limits": {"catalog": 1, "telemetry": 2, "merge": 1, "legacy": 1, "default": 2},
  "retry_backoff": 1,
  "backoff_multiplier": 2,
  "aging_step": 1,
  "cancellations": {"report": 10}
}
```

- `max_parallel`：总执行槽上限；`group_limits`：各组槽上限（自动截断到总上限）；未列出的组使用 `default_group_limit`，再退回 `max_parallel`。
- `retry_backoff` / `backoff_multiplier`：第 n 次失败后的退避为 `base * multiplier^(n-1)`。
- `aging_step`：公平性老化系数，`0` 即严格优先级。
- `cancellations`：`task_id -> 相对秒数`，到点后取消该任务；也支持在构造调度器时传入外部取消回调。

## 调度语义

1. **依赖感知**：任务仅当 `requires` 全部 `completed` 后才进入就绪集，优先级不能让前置未完成的后续任务抢先。高优先级全量抓取未结束时，其依赖任务不会先跑。
2. **资源门控**：就绪任务必须同时满足总槽与所属组槽才可开始。执行槽只在尝试真正运行时占用。
3. **重试重新参与分配**：尝试失败立刻释放槽位并进入 `backoff`；退避只等时钟，不占执行槽，也不阻塞其他来源。到点后重新进入就绪集，与其它就绪任务同样竞争资源。
4. **就绪任务排序**：有效优先级 = `priority + aging_step * 已等待秒数`。同时就绪时按原始优先级；长期等待的低优先级任务会逐步追上，避免组内饿死。排序并列时按原始优先级、就绪时间、任务 id 稳定裁决。
5. **取消与前置完成同时发生**：每轮先处理取消、再处理本轮尝试结果、再传播阻塞、再提升就绪、最后派发。因此同一时刻观察到"取消 + 前置完成"时，取消获胜：被取消任务不会启动，其后续任务记录 `dependency_cancelled:<id>`；已被取消的尝试仍按其真实结果记入尝试表。在更早轮次已终态的任务不受之后才观察到的取消影响。
6. **明确阻塞原因**（不会一直显示等待）：
   - `dependency_failed:<id>`：前置重试耗尽；
   - `dependency_cancelled:<id>`：前置被取消；
   - `dependency_blocked:<id>`：最近一个阻塞前置（沿链回溯根因）；
   - `dependency_missing:<id>`：存在缺失引用（沿链传播根因）；
   - `dependency_cycle`：任务位于依赖环中（沿链传播根因）。
7. 分配名额的时机：仅在实际派发尝试时占用；尝试结束（成功/失败/取消，含执行器抛错）立即释放；退避、等待、阻塞、终态均不占槽。

## 运行存储

`RunStore` 记录三张视图并保持一致：

- `task_runs`：每任务最新状态、尝试次数、资源组、`blocked_reason`、起止时间；
- `task_attempts`：每次尝试一行（`running` → `succeeded` / `failed` / `cancelled`），含组、起止时间和详情；
- `task_events`：完整轨迹（`waiting`、`ready`、`started`、`attempt_finished`、`backoff`、`retry_ready`、`blocked`、`cancel_requested`、`cancelled`、`completed`、`failed`）；
- `run_usage`：总槽与各组槽的历史水位（high watermark），用于核对限额从未超标；
- `runs`：运行级配置、开始/结束时间与汇总。

旧库（仅 `task_runs(task_id,status,attempts,mode)` 与 `task_attempts(task_id,attempt,status,mode)`）打开时自动 `ALTER TABLE` 补齐新列。

## 使用

```bash
# 旧 fixture（无需配置）
PYTHONPATH=src python3 -m orchestrator.cli --fixture fixtures/tasks.json --database /tmp/runs.sqlite3

# 带依赖、资源组、退避与取消的演示（虚拟时钟，瞬间完成且可复现）
PYTHONPATH=src python3 -m orchestrator.cli \
  --fixture fixtures/dynamic_tasks.json \
  --config fixtures/run_config.json \
  --database /tmp/dynamic.sqlite3 --virtual-time

# 测试
PYTHONPATH=src python3 -m pytest tests
```

`fixtures/invalid_tasks.json` 覆盖循环依赖、缺失引用及其下游任务的阻塞原因。

## 外部依赖类型与版本

- Python 3.11+
- pytest 8.3.x（开发/测试）
