"""可直接 uvicorn 启动的离线任务 API（接入真实运行的最小装配）。

    python -m uvicorn examples.serve:app --port 8090

数据落在 CP_STORE_ROOT / CP_DB_PATH（默认 /tmp）。推理引擎仍是 MockEngine——
真实部署时把 worker 的 MockEngine 换成 vLLM-Ascend 适配即可，API/调度逻辑不变。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from compute_platform.config import Config
from compute_platform.objectstore import LocalObjectStore
from compute_platform.queue import SqliteShardQueue
from compute_platform.registry import ModelRegistry
from compute_platform.batch_api.service import JobStore, BatchService
from compute_platform.batch_api.app import create_app
from compute_platform.governance import QuotaManager

_cfg = Config()
_store = LocalObjectStore(_cfg.store_root)
_queue = SqliteShardQueue(_cfg.db_path, max_retries=_cfg.max_retries)
_registry = ModelRegistry.with_defaults()
_quota = QuotaManager({"default": 64, "data-team": 256})
_service = BatchService(_cfg, _store, _queue, _registry, JobStore(_cfg.db_path), _quota)

app = create_app(_service)
