"""运行配置，全部来自环境变量，便于容器注入。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    # JSONL 段文件的挂载根目录（容器内可写，归并完成后要回收段文件）
    mount_dir: Path
    # 作业状态与已发布页的持久化目录
    work_dir: Path

    @staticmethod
    def from_env() -> "Settings":
        mount = Path(os.environ.get("MERGE_MOUNT_DIR", "./sample/input")).resolve()
        work = Path(os.environ.get("MERGE_WORK_DIR", "./data")).resolve()
        return Settings(mount_dir=mount, work_dir=work)
