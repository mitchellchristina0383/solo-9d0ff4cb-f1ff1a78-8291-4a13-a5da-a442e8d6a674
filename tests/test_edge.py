"""边界与完整性测试：空段、单段、大页、重复键跨页、整批不漏不重。"""

from __future__ import annotations

import json
from pathlib import Path

from app.engine import MergeService, page_filename
from app.models import JobCreate, SegmentSpec

from .conftest import make_job, write_segment


def _all_records(work: Path, job_id: str, n_pages: int) -> list[dict]:
    out: list[dict] = []
    for no in range(1, n_pages + 1):
        p = work / job_id / page_filename(no)
        out.extend(json.loads(line) for line in p.read_text().splitlines() if line.strip())
    return out


def test_empty_and_single_segment(service: MergeService, dirs):
    mount, work = dirs
    write_segment(mount, "empty.jsonl", [])
    write_segment(mount, "only.jsonl", [1, 2, 3])
    req = JobCreate(job_id="edge1", key_field="key", page_size=2, segments=[
        SegmentSpec(path="empty.jsonl", order=0),
        SegmentSpec(path="only.jsonl", order=1),
    ])
    service.register_job(req)
    r = service.run_to_end("edge1")
    assert r.finished and r.published == [1, 2]
    got = _all_records(work, "edge1", 2)
    assert [x["key"] for x in got] == [1, 2, 3]
    # 空段也应被回收
    assert not (mount / "empty.jsonl").exists()
    assert not (mount / "only.jsonl").exists()


def test_page_size_larger_than_total(service: MergeService, dirs):
    mount, work = dirs
    req = make_job("edge2", mount, [[1, 2], [3]], page_size=100)
    service.register_job(req)
    r = service.run_to_end("edge2")
    assert r.published == [1]
    status = service.get_status("edge2")
    assert status.page_counts == [3]  # 唯一一页即末页，允许短


def test_duplicate_keys_span_pages(service: MergeService, dirs):
    mount, work = dirs
    # 5 条同键，分散在两路，page_size=2，同键跨页也必须保持路序
    req = make_job("edge3", mount, [[7, 7, 7], [7, 7]], page_size=2)
    service.register_job(req)
    service.run_to_end("edge3")
    got = _all_records(work, "edge3", 3)
    assert [x["key"] for x in got] == [7, 7, 7, 7, 7]
    assert [x["payload"] for x in got] == [
        "r0-0", "r0-1", "r0-2", "r1-0", "r1-1"]


def test_no_loss_or_duplication_larger_run(service: MergeService, dirs):
    """较大数据量：逐页推进 + 中途多次“重启”，校验全量 multiset 一致。"""
    mount, work = dirs
    keys0 = list(range(0, 200, 2))
    keys1 = list(range(1, 200, 2))
    req = make_job("edge4", mount, [keys0, keys1], page_size=7)
    service.register_job(req)

    expected = sorted(keys0 + keys1)
    for step in range(50):
        # 每三轮换一个服务实例，模拟进程重启后接着跑
        svc = MergeService(mount, work)
        svc.advance("edge4")
        if svc.get_status("edge4").finished:
            break

    status = MergeService(mount, work).get_status("edge4")
    got = [r["key"] for r in _all_records(work, "edge4", status.published_pages)]
    assert got == expected                      # 有序且不漏不重
    assert len(got) == len(set(got)) == 200
    assert status.finished
    assert all(s.reclaimed for s in status.segments)


def test_blank_lines_tolerated(service: MergeService, dirs):
    mount, work = dirs
    p = mount / "blanks.jsonl"
    p.write_text(
        json.dumps({"key": 1}) + "\n\n" + json.dumps({"key": 2}) + "\n\n",
        encoding="utf-8",
    )
    req = JobCreate(job_id="edge5", key_field="key", page_size=10, segments=[
        SegmentSpec(path="blanks.jsonl", order=0)])
    service.register_job(req)
    service.run_to_end("edge5")
    got = _all_records(work, "edge5", 1)
    assert [r["key"] for r in got] == [1, 2]


def test_concurrent_advances_are_serialized(dirs):
    """同作业并发推进：作业锁串行化，页数与行数仍然精确不重不漏。"""
    import threading

    mount, work = dirs
    req = make_job("edge6", mount,
                   [list(range(0, 60, 2)), list(range(1, 60, 2))], page_size=5)
    svc = MergeService(mount, work)
    svc.register_job(req)

    errors: list[Exception] = []

    def worker() -> None:
        local = MergeService(mount, work)  # 新实例共享同一磁盘状态与进程内锁表
        try:
            for _ in range(20):
                r = local.advance("edge6")
                if r.finished:
                    return
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors
    status = svc.get_status("edge6")
    assert status.finished and status.total_published_rows == 60
    got = [r["key"] for r in _all_records(work, "edge6", status.published_pages)]
    assert got == list(range(60))
    assert sum(status.page_counts) == 60
