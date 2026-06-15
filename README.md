# 算力服务化平台 — 混部调度核心（可运行实现）

> **最新方向（v2）**：按《[NPU统一推理算力池需求说明_v2](./docs/NPU统一推理算力池需求说明_v2.md)》，
> 问题已从"两类业务抢卡"重定位为"统一多模型推理算力池"。重新设计的实现见
> `src/inference_pool/`，设计说明见《[NPU统一推理算力池设计_v2](./docs/NPU统一推理算力池设计_v2.md)》。
> 下方 `compute_platform` 为旧实现，其幂等分片队列被新实现复用，三池配额/抢占等模块已标注 deprecated。

## inference_pool（v2 新实现）

| 需求层 | 模块 |
|---|---|
| 推理混跑（GLM5.1 单部署、在线插队 + 批量填谷） | `inference_pool.gateway` |
| 在线副本自动扩缩（64–3000、温卡优先、预热、缩容迟滞） | `inference_pool.autoscaler` + `inference_pool.card_pool` |
| 多模型离线编排（换装聚合 + 幂等分片续跑 + deadline） | `inference_pool.offline_pool` |
| prefix/KV cache 亲和路由（Claude Code 专项） | `inference_pool.cache_affinity` |
| 顶层控制回路 | `inference_pool.platform` |

---

## compute_platform（旧实现，离线批处理子集仍复用）

按《[算力服务化平台架构设计v2.0](./docs/算力服务化平台架构设计v2.0.md)》实现的离线批处理 + 调度核心（自研混部薄层的策略内核参考实现），
不依赖昇腾硬件即可全量测试。真实环境组件在此用替身：

| 真实组件 | 本实现替身 |
|---|---|
| vLLM-Ascend / MindIE | `inference.MockEngine`（本地 in-process generate） |
| 统一对象存储 | `objectstore.LocalObjectStore`（原子写 + range read） |
| Redis / PostgreSQL 队列 | `queue.SqliteShardQueue`（BEGIN IMMEDIATE 原子 claim + 租约） |
| Volcano / KEDA | `scheduler.controller` / `scheduler.preemption` / `scheduler.pools`（纯函数策略） |
| DataHub（血缘/版本/region） | `governance.lineage.LineageRegistry`（进程内版本链） |

详尽设计见《[算力服务化平台详细设计v1.0](./docs/算力服务化平台详细设计v1.0.md)》。

## 运行

```bash
# 复用任意装有 fastapi/pydantic/httpx/pytest 的 venv
python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

# 全量测试（64 用例）
python -m pytest

# 端到端 demo（进程内拉起全链路：提交→切分→4 worker 并发→100%→计量）
python examples/quickstart.py

# 启动离线任务 API（端口 8090）
python -m uvicorn examples.serve:app --port 8090   # 见 examples/serve.py
```

## 代码结构

```
src/compute_platform/
├── config.py            # 配置（CP_ 前缀）：租约/粒度/门槛 K/计价
├── models.py            # 领域模型：QoS · CardType · Job · Shard · ModelSpec
├── objectstore.py       # 对象存储：原子写、range read
├── registry.py          # 模型注册表：模型→卡型/加载耗时/吞吐/占卡
├── sharder.py           # 切分器：切指针不切数据、粒度反推、可重入
├── inference.py         # 推理引擎抽象 + MockEngine（§4.4 扩展点）
├── worker.py            # 离线 worker 主循环（嵌入式推理、幂等提交）
├── queue/               # 分片队列 + 租约（系统心脏）
│   ├── base.py          #   接口：claim/renew/commit/abort/fail/reap...
│   └── sqlite_queue.py  #   SQLite 实现（Redis/PG 的替身）
├── scheduler/           # 调度核心（纯函数，便于测试）
│   ├── controller.py    #   弹性伸缩：desired=min(积压,供给,配额)+加载门槛+末班车
│   ├── preemption.py    #   抢占 victim 选择：只抢突发池、先小后大、护新大实例
│   ├── pools.py         #   三池模型（决策三）：在线保障/离线配额/弹性突发 + 配额打标
│   ├── locality.py      #   数据局部性硬约束：PB 不跨域、强制就近
│   └── fairshare.py     #   跨产线加权公平分配（注水法、整数守恒）
├── governance/          # 治理
│   ├── quota.py         #   多租户卡级配额硬边界
│   ├── metering.py      #   差异计量：卡型差价 + 突发折扣 + 被抢不计费
│   └── lineage.py       #   DataHub 替身：版本链 + 端到端血缘 + region
└── batch_api/           # 离线作业接入（服务化边界=任务提交，非每次推理）
    ├── service.py       #   提交/进度/错误/取消/重试 + JobStore + 幂等
    │                    #   + 数据局部性强制 + 完成回写血缘(task×DataHub 闭环)
    └── app.py           #   FastAPI 五接口
```

## 测试覆盖（102 用例全绿）

| 文件 | 覆盖 |
|---|---|
| test_objectstore | 原子写、range read、防逃逸、覆盖幂等 |
| test_queue | 幂等入队、原子 claim、租约过期回收、CAS commit、fail→死信、abort、取消、模型隔离 |
| test_sharder | 指针分片、粒度反推、范围正确、确定性 |
| test_worker | 全量处理、幂等覆盖、毒分片→死信、优雅退出 |
| test_controller | 积压/供给/配额三约束、加载成本门槛、潮汐末班车 |
| test_preemption | 不抢 Guaranteed/Protected/Fixed、先小后大、护新大实例、三池只抢超配额 |
| test_pools | 三池容量约束、配额内/跨界/超配打标、排空余量、负值防御 |
| test_locality | 就近放置、本域故障拦截跨域、显式放行、缺 region 拒绝 |
| test_fairshare | 按权重比例、want 封顶再分配、整数守恒、零卡/零权重 |
| test_lineage | 版本登记、父版本校验、多轮链路回溯、latest、复现校验、跨产线多父 |
| test_governance | 配额 reserve/release/超限、差异计价、Protected 全价、被抢不计费、按租户结算 |
| test_batch_api | 提交、未知模型 400、配额 429、幂等 token、进度、错误/重试、取消、404 |
| test_service_governance | 局部性拦截/放行、缺父版本拒绝、两轮血缘闭环、未完成禁 complete |
| test_end_to_end | 单 worker 全链路、抢占后续跑不丢、4 worker 并发无重复无丢失 |
