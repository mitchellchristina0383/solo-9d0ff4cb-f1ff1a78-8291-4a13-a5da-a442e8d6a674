"""pytest 公共夹具。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.config import Settings
from app.engine import MergeService
from app.main import create_app
from app.models import JobCreate, SegmentSpec


@pytest.fixture()
def dirs(tmp_path: Path) -> tuple[Path, Path]:
    mount = tmp_path / "mount"
    work = tmp_path / "work"
    mount.mkdir()
    work.mkdir()
    return mount, work


@pytest.fixture()
def service(dirs: tuple[Path, Path]) -> MergeService:
    mount, work = dirs
    return MergeService(mount_dir=mount, work_dir=work)


def write_segment(mount: Path, rel: str, keys: list[int], extra: str = "v") -> Path:
    """写一路已排序 JSONL；同键可重复，payload 带行号便于断言顺序。"""
    path = mount / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for i, k in enumerate(keys):
            f.write(json.dumps({"key": k, "payload": f"{extra}-{i}"}) + "\n")
    return path


def make_job(
    job_id: str,
    mount: Path,
    seg_keys: list[list[int]],
    page_size: int = 3,
) -> JobCreate:
    segs = []
    for order, keys in enumerate(seg_keys):
        rel = f"segments/run-{order}.jsonl"
        write_segment(mount, rel, keys, extra=f"r{order}")
        segs.append(SegmentSpec(path=rel, order=order))
    return JobCreate(
        job_id=job_id, key_field="key", page_size=page_size, segments=segs
    )


@pytest.fixture()
def client(dirs: tuple[Path, Path]):
    from fastapi.testclient import TestClient

    mount, work = dirs
    app = create_app(Settings(mount_dir=mount, work_dir=work))
    with TestClient(app) as c:
        yield c
