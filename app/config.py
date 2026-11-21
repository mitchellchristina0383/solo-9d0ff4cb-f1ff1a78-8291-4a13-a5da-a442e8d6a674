"""运行配置，全部来自环境变量，便于容器注入。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# 服务同时打开的输入段文件数上限；Docker Compose 启动时通过
# MERGE_MAX_OPEN_INPUT_FILES 覆盖。登记校验、单步推进、连续运行共用此额度。
DEFAULT_MAX_OPEN_INPUT_FILES = 16
MAX_OPEN_ENV = "MERGE_MAX_OPEN_INPUT_FILES"


@dataclass(frozen=True)
class Settings:
    # JSONL 段文件的挂载根目录（容器内可写，归并完成后要回收段文件）
    mount_dir: Path
    # 作业状态与已发布页的持久化目录
    work_dir: Path
    # 同时打开的输入段文件数上限（进程级，所有作业/所有推进共用）
    max_open_input_files: int = DEFAULT_MAX_OPEN_INPUT_FILES

    def __post_init__(self) -> None:
        value = self.max_open_input_files
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{MAX_OPEN_ENV} 必须是正整数，收到 {value!r}")

    @staticmethod
    def from_env() -> "Settings":
        mount = Path(os.environ.get("MERGE_MOUNT_DIR", "./sample/input")).resolve()
        work = Path(os.environ.get("MERGE_WORK_DIR", "./data")).resolve()
        raw = os.environ.get(MAX_OPEN_ENV, "").strip()
        if raw:
            try:
                limit = int(raw)
            except ValueError as e:
                raise ValueError(f"{MAX_OPEN_ENV} 必须是正整数，收到 {raw!r}") from e
        else:
            limit = DEFAULT_MAX_OPEN_INPUT_FILES
        return Settings(mount_dir=mount, work_dir=work, max_open_input_files=limit)
