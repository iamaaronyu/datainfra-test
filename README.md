# datainfra-test — NPU 算力服务化 / 统一推理算力池 参考实现

不依赖昇腾硬件即可全量测试的策略内核参考实现。仓库包含**两代设计**及其可运行代码：

- **v2（最新）`inference_pool`** — 按《[NPU 统一推理算力池需求 v2](./docs/20-NPU统一推理算力池_需求_v2.md)》，
  把问题从"两类业务抢卡"重定位为**统一多模型推理算力池**。
- **v1（旧）`compute_platform`** — 按《[算力服务化平台架构设计 v2.0](./docs/11-算力服务化平台_架构设计_v2.0.md)》，
  面向离线批处理的"三池配额 + 搬卡抢占"调度核心。其幂等分片队列被 v2 复用，配额/抢占类模块已标注 deprecated。

> 为什么有两代？需求澄清后问题性质变了：在线(Claude Code 调 GLM5.1)与离线批量**本质是同一种推理负载**，
> 是正和而非零和。详见《[NPU 统一推理算力池设计 v2](./docs/21-NPU统一推理算力池_设计_v2.md)》§1。

---

## 文档（`docs/`）

命名规范 `编号-产品名_类型_版本.md`：`10/11/12` 为旧套，`20/21` 为新套。

| 文档 | 归属 | 说明 |
|---|---|---|
| [10-数据工程平台_需求_v2.0](./docs/10-数据工程平台_需求_v2.0.md) | v1 | 旧需求基线 |
| [11-算力服务化平台_架构设计_v2.0](./docs/11-算力服务化平台_架构设计_v2.0.md) | v1 | 旧总体/架构设计 |
| [12-算力服务化平台_详细设计_v1.0](./docs/12-算力服务化平台_详细设计_v1.0.md) | v1 | 旧详细设计（对应 `compute_platform`） |
| [20-NPU统一推理算力池_需求_v2](./docs/20-NPU统一推理算力池_需求_v2.md) | **v2** | **最新需求**（R1–R5 + §5 八大场景 + §10 差异说明） |
| [21-NPU统一推理算力池_设计_v2](./docs/21-NPU统一推理算力池_设计_v2.md) | **v2** | **最新技术设计**（模块划分 + 需求↔代码↔测试映射） |

**建议阅读顺序**：先 `20`（要什么）→ 再 `21`（怎么做、怎么落到代码）。

---

## 代码（`src/`）

### v2：`inference_pool` —— 统一推理算力池（最新实现）

三层架构（消解了旧需求 ~80% 复杂度），由 `platform.py` 串成一个控制回路：

| 模块 | 职责 | 对应需求 |
|---|---|---|
| [`models.py`](./src/inference_pool/models.py) | 领域模型：卡角色状态机 / 优先级 / 模型规格 / 分片任务 | — |
| [`gateway.py`](./src/inference_pool/gateway.py) | **推理混跑层**：GLM5.1 单部署，在线请求实时插队、批量填谷，连续批处理，**不搬卡** | R1 |
| [`autoscaler.py`](./src/inference_pool/autoscaler.py) | **在线副本自动扩缩**：64–3000 卡，负载驱动 + 预测预热 + 缩容迟滞 | R2 |
| [`card_pool.py`](./src/inference_pool/card_pool.py) | **卡池状态机**：温卡优先回收（秒级）→ 换装（分钟级）递进 | R2/§3 |
| [`offline_pool.py`](./src/inference_pool/offline_pool.py) | **多模型离线编排**：换装聚合 + 幂等分片续跑 + deadline（复用 `compute_platform.queue`） | R3/R5 |
| [`cache_affinity.py`](./src/inference_pool/cache_affinity.py) | **prefix/KV cache 亲和路由**（Claude Code 专项） | R4 |
| [`platform.py`](./src/inference_pool/platform.py) | 顶层控制回路，保证"在线 SLA 优先"硬不变量 | §3/§5 |

### v1：`compute_platform` —— 旧离线批处理调度核心

```
src/compute_platform/
├── models.py / config.py / registry.py   领域模型 · 配置 · 模型注册表
├── objectstore.py                        对象存储（原子写 + range read）
├── sharder.py                            切分器（切指针不切数据、可重入）
├── inference.py / worker.py              推理引擎抽象 + 离线 worker 主循环
├── queue/                                幂等分片队列 + 租约 ★ v2 复用此件
├── scheduler/                            调度策略（纯函数）
│   ├── controller.py · locality.py       弹性伸缩 · 数据局部性约束（仍有效）
│   └── pools.py · preemption.py · fairshare.py   三池/抢占/公平分配（⚠ deprecated）
├── governance/                           治理：quota(⚠ deprecated) · metering · lineage
└── batch_api/                            离线作业 FastAPI 接入
```

> `scheduler/{pools,preemption,fairshare}.py` 与 `governance/quota.py` 是旧"业务百分比配额 +
> 搬卡抢占"模型，已在 docstring 标注 deprecated；v2 改由 autoscaler 定在线副本数、推理层优先级混跑。

---

## 运行

```bash
# 装依赖（任意装有 fastapi/pydantic/httpx/pytest 的 venv 均可）
python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

# 全量测试（137 用例：旧 102 + 新 35）
python -m pytest

# 只跑 v2 统一推理算力池
python -m pytest tests/inference_pool

# v1 端到端 demo（进程内拉起全链路：提交→切分→4 worker 并发→100%→计量）
python examples/quickstart.py

# v1 离线任务 API（端口 8090）
python -m uvicorn examples.serve:app --port 8090   # 见 examples/serve.py
```

---

## 测试覆盖

### v2 `tests/inference_pool/`（35 用例）

| 文件 | 覆盖 |
|---|---|
| test_gateway | 在线优先级插队、批量填谷、不被批量饿死、高峰压批量、过载信号 |
| test_autoscaler | QPS/队列深度驱动、上下限夹取、TTFT 追加、扩容即时、缩容迟滞、预测预热 |
| test_card_pool | 温卡优先回收、换装时序、缩容回流温卡、缺口处理、在线不变量 |
| test_cache_affinity | 冷会话 miss→后续 hit、粘性路由、缩容仅失效被移除副本、无会话不亲和 |
| test_offline_pool | 幂等入队、卡型成组、派发提交、drain 续跑、回收退还分片、deadline 排序 |
| test_scenarios | **端到端逐一对应需求 §5.1–5.8 八大场景** |

### v1 `tests/`（102 用例）

队列 / 切分 / worker / 控制器 / 抢占 / 三池 / 局部性 / 公平分配 / 血缘 / 计量 / 配额 / batch_api / 端到端，详见各 `test_*.py`。
