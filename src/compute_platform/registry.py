"""模型注册表（控制面，§4.3）。

记录 模型 → 卡型 / 加载耗时 / 吞吐 / 占卡数。被切分器用来反推分片粒度，
被弹性控制器用来算加载成本门槛。
"""
from __future__ import annotations

from .models import CardType, ModelSpec


class ModelRegistry:
    def __init__(self) -> None:
        self._models: dict[str, ModelSpec] = {}

    def register(self, spec: ModelSpec) -> None:
        self._models[spec.name] = spec

    def get(self, name: str) -> ModelSpec:
        if name not in self._models:
            raise KeyError(f"unknown model: {name}")
        return self._models[name]

    def exists(self, name: str) -> bool:
        return name in self._models

    def names(self) -> list[str]:
        return sorted(self._models)

    @classmethod
    def with_defaults(cls) -> "ModelRegistry":
        """贵阳试点默认模型集（设计 §2.1/§2.2）。"""
        r = cls()
        r.register(ModelSpec("GLM-5.1", CardType.B3, cards_per_worker=4,
                             load_seconds=300, throughput_rows_per_sec=40))
        r.register(ModelSpec("Qwen3.5-VL-235B", CardType.B3, cards_per_worker=8,
                             load_seconds=900, throughput_rows_per_sec=20))
        r.register(ModelSpec("DeepSeek-V4", CardType.B3, cards_per_worker=16,
                             load_seconds=900, throughput_rows_per_sec=15))
        r.register(ModelSpec("small-7B", CardType.B4, cards_per_worker=1,
                             load_seconds=30, throughput_rows_per_sec=120))
        return r
