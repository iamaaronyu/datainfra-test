"""推理引擎抽象（§4.4 扩展点：vLLM-Ascend 与 MindIE 通过统一 generate 接入）。

设计关键（§6.1）：离线 worker 把引擎嵌进进程，`model.generate(batch)` 是本地
函数调用而非远端 API。这里用 MockEngine 替身，使全量测试不依赖昇腾硬件。
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class InferenceEngine(ABC):
    @abstractmethod
    def load(self) -> None:
        """加载模型常驻显存（真实环境 235B 十几分钟）。"""

    @abstractmethod
    def generate(self, batch: list[str]) -> list[str]:
        """本地 in-process 批量推理。输入一批、返回等长一批。"""


class MockEngine(InferenceEngine):
    """确定性替身：输出可预测，便于断言；遇到含 POISON 的行抛错（测毒分片）。"""

    POISON = "POISON"

    def __init__(self, model: str):
        self.model = model
        self.loaded = False
        self.generated_rows = 0

    def load(self) -> None:
        self.loaded = True

    def generate(self, batch: list[str]) -> list[str]:
        if not self.loaded:
            raise RuntimeError("engine not loaded")
        out = []
        for row in batch:
            if self.POISON in row:
                raise ValueError(f"poison row triggers engine error: {row[:32]}")
            out.append(f"[{self.model}] {row.strip()}")
        self.generated_rows += len(batch)
        return out
