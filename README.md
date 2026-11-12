# 多来源采集任务编排与运行治理平台

按单一职责拆分为：`sources.py` 读取任务，`scheduler.py` 负责优先级与重试策略，`storage.py` 负责运行状态入库，`pipeline.py` 负责编排，`application.py` 负责依赖组装，`cli.py` 负责命令行。

默认使用本地 fixture，验证全量/增量模式字段、优先级排序、失败重试和运行状态入库，不连接外部服务。

## 外部依赖类型与版本

- Python 3.11+
- pytest 8.3.x（开发/测试）
