# NPU 统一推理算力池 — 技术设计与实现说明 (v2)

> 对应需求《[NPU统一推理算力池需求说明_v2](./NPU统一推理算力池需求说明_v2.md)》。
> 本文说明为落地该需求**重新设计的技术栈**、模块划分、与旧 `compute_platform` 的关系，
> 以及代码实现（`src/inference_pool/`）与测试的映射。

---

## 1. 为什么要重做：旧架构与新需求的根本错配

旧 `compute_platform` 是按"两类业务抢同一池卡"的 HPC 框架做的：**三池配额
（GUARANTEED/PROTECTED/PREEMPTIBLE）+ 搬卡式抢占**。新需求把问题重新定位为
"统一多模型推理算力池"，核心区别是：

| 维度 | 旧实现（compute_platform） | 新实现（inference_pool） |
|---|---|---|
| 在线/离线关系 | **零和**：抢同一批物理卡 | **正和**：同一批副本优先级混跑，离线填在线空泡 |
| 解决手段 | 物理"搬卡"+抢占顺序+checkpoint 恢复 | 软件"调请求优先级"+卡角色转换 |
| 调度对象 | 业务百分比配额 | **在线副本卡数**（autoscale 决定） |
| 离线中断 | checkpoint 状态机 | **幂等分片续跑**（无 checkpoint） |

---

## 2. 技术栈与模块划分

新代码独立成包 `src/inference_pool/`，**只复用旧实现里方向一致的资产**
（幂等分片队列 `compute_platform.queue`），其余全部重写。

```
inference_pool/
├── models.py         领域模型：CardType / CardRole 状态 / Priority / ModelSpec / Request / ShardTask
├── card_pool.py      卡池状态机（§2 三层 / §3 不变量）：温卡优先回收 + 换装时序
├── autoscaler.py     在线副本自动扩缩（§6.2 关键路径）：负载驱动 + 预测预热 + 缩容迟滞
├── gateway.py        推理混跑层（§6.1）：GLM5.1 单部署，在线插队 + 批量填谷，连续批处理
├── cache_affinity.py prefix/KV cache 亲和路由（§6.4，Claude Code 专项）
├── offline_pool.py   多模型离线编排（§6.3）：换装聚合 + 幂等分片 + drain 续跑 + deadline
└── platform.py       顶层控制回路，把以上串成一个周期
```

**技术选型**：纯 Python + dataclass 状态机，时钟可注入（`FakeClock`）以便对"换装分钟级、
温卡秒级、缩容冷却"等**时序语义**做确定性测试，不依赖真实 sleep / 昇腾硬件。幂等分片队列
沿用 SQLite（`BEGIN IMMEDIATE` 原子 claim + 租约），生产可换 Redis/PostgreSQL。

---

## 3. 需求 → 实现 → 测试 映射

| 需求 | 实现 | 测试 |
|---|---|---|
| R1 推理混跑层（单部署/优先级/填谷/可压制） | `gateway.py` | `test_gateway.py` |
| R2 在线副本自动扩缩（64–3000/温卡/预热/迟滞） | `autoscaler.py` + `card_pool.scale_online` | `test_autoscaler.py` `test_card_pool.py` |
| R3 多模型离线编排（换装/幂等分片/drain 续跑） | `offline_pool.py`（复用 `compute_platform.queue`） | `test_offline_pool.py` |
| R4 prefix/KV cache 亲和路由 | `cache_affinity.py` | `test_cache_affinity.py` |
| R5 卡型 B3/B4 + 降级 | `models.py` + `offline_pool.place` | `test_offline_pool.py::test_place_prefers_required_card_type` |
| §5.1–5.8 八大场景 | `platform.py` 控制回路 | `test_scenarios.py`（逐场景） |

---

## 4. 核心不变量如何被保证（§3）

1. **在线 SLA 优先**：`card_pool` 换装中的卡在 `tick()` 就位前**不计入** `online_cards()`，
   `gateway` 只拿 `instant_online`（温卡秒级到位），绝不超喂；deadline 任务只在离线池内竞争
   （`offline_pool.urgent_jobs`），`autoscaler` 仅按在线负载决策，与离线无关。
2. **混跑不搬卡**：`gateway.step()` 每步先排满在线、批量只取剩余 slot；平稳期不暂停批量（填谷）。
3. **温卡优先**：`scale_online` 先回收 `WARM_BATCH`（秒级），不足才 reload `OFFLINE`（分钟级）。
4. **离线幂等**：`offline_pool.drain_card` 把在途分片 `abort` 回队列，换机 `dispatch` 续跑，无 checkpoint。

---

## 5. 与旧模块的关系（迁移说明）

- **复用**：`compute_platform.queue`（幂等分片队列）、`compute_platform.sharder`（切指针不切数据）。
- **废弃**（已在各模块 docstring 标注 deprecated，仍保留供旧 compute_platform 测试与离线子集用）：
  `scheduler/pools.py`（三池配额）、`scheduler/fairshare.py`（业务权重分配）、
  `scheduler/preemption.py`（搬卡抢占）、`governance/quota.py`（百分比配额）。
- 旧 102 用例与新 35 用例**全部保留并通过**（共 137 绿），确保重构不破坏既有离线能力。
