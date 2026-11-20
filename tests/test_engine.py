"""引擎级测试：归并顺序、分页、回收、崩溃恢复。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.engine import (
    EngineError,
    JobNotFound,
    MergeService,
    page_filename,
)
from app.models import JobCreate

from .conftest import make_job, write_segment


def _read_page(work: Path, job_id: str, page_no: int) -> list[dict]:
    path = work / job_id / page_filename(page_no)
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _all_published(work: Path, job_id: str, n_pages: int) -> list[dict]:
    out: list[dict] = []
    for no in range(1, n_pages + 1):
        out.extend(_read_page(work, job_id, no))
    return out


def expected_order(seg_keys: list[list[int]]) -> list[tuple[int, int]]:
    """期望输出：(key, 路序) 排序，路内保持原行序。"""
    rows = [(k, order) for order, keys in enumerate(seg_keys) for k in keys]
    return sorted(rows, key=lambda x: (x[0], x[1]))


# --------------------------------------------------------------------- #
# 1. 基本归并顺序：跨段同键按路序，路内按原行序
# --------------------------------------------------------------------- #
def test_merge_order_with_cross_segment_duplicate_keys(
    service: MergeService, dirs, tmp_path
):
    mount, work = dirs
    seg_keys = [
        [1, 5, 5, 9],      # 路0：key=5 两条
        [2, 5, 7],         # 路1：key=5 一条
        [3, 5, 8, 9],      # 路2：key=5、9 各一
    ]
    req = make_job("j1", mount, seg_keys, page_size=2)
    service.register_job(req)
    result = service.run_to_end("j1")

    assert result.finished is True
    assert result.published == [1, 2, 3, 4, 5, 6]  # 11 行 / 每页 2 -> 6 页
    got = [(r["key"], int(r["payload"].split("-")[0][1:]))
           for r in _all_published(work, "j1", 6)]
    assert got == expected_order(seg_keys)

    # 显式断言同键顺序：4 条 key=5 依次来自路0(两行)、路1、路2
    key5 = [g[1] for g in got if g[0] == 5]
    assert key5 == [0, 0, 1, 2]
    # key=9：路0 在路2 前
    key9 = [g[1] for g in got if g[0] == 9]
    assert key9 == [0, 2]


# --------------------------------------------------------------------- #
# 2. 分页：除末页外每页满页，只有末页可短
# --------------------------------------------------------------------- #
def test_paging_only_last_page_may_be_short(service: MergeService, dirs):
    mount, work = dirs
    req = make_job("j2", mount, [[1, 3, 5], [2, 4]], page_size=3)
    service.register_job(req)
    r1 = service.advance("j2")
    assert (r1.page_no, r1.rows, r1.finished) == (1, 3, False)
    r2 = service.advance("j2")
    assert (r2.page_no, r2.rows, r2.finished) == (2, 2, True)
    # 再推进无新页
    r3 = service.advance("j2")
    assert r3.page_no is None and r3.rows == 0 and r3.finished is True

    status = service.get_status("j2")
    assert status.page_counts == [3, 2]
    assert status.total_published_rows == 5


# --------------------------------------------------------------------- #
# 3. 每路只留一条待比较记录（结构性检查：堆大小不超过路数）
# --------------------------------------------------------------------- #
def test_heap_never_holds_more_than_one_record_per_run(
    service: MergeService, dirs, monkeypatch
):
    mount, work = dirs
    req = make_job("j3", mount, [list(range(10, 20)), list(range(15, 25)),
                                 list(range(0, 5))], page_size=4)
    service.register_job(req)

    import app.engine as eng
    max_heap = 0
    real_push = eng.heapq.heappush

    def spy_push(heap, item):
        real_push(heap, item)
        nonlocal max_heap
        max_heap = max(max_heap, len(heap))

    monkeypatch.setattr(eng.heapq, "heappush", spy_push)
    service.run_to_end("j3")
    assert max_heap <= 3  # 三路，任意时刻每路至多一条在堆


# --------------------------------------------------------------------- #
# 4. 回收：段记录全部发布后段文件被删除，未消费完的保留
# --------------------------------------------------------------------- #
def test_reclaim_segment_after_fully_published(service: MergeService, dirs):
    mount, work = dirs
    # 路0 很短，第一页（3 行）发布后路0 即耗尽
    req = make_job("j4", mount, [[1, 2], [3, 4, 5, 6, 7]], page_size=3)
    service.register_job(req)
    seg0 = mount / "segments/run-0.jsonl"
    seg1 = mount / "segments/run-1.jsonl"

    r1 = service.advance("j4")
    assert "segments/run-0.jsonl" in r1.reclaimed
    assert not seg0.exists()          # 已回收
    assert seg1.exists()              # 路1 仍有未发布记录

    service.run_to_end("j4")
    assert not seg1.exists()
    status = service.get_status("j4")
    assert all(s.reclaimed for s in status.segments)
    assert status.finished


# --------------------------------------------------------------------- #
# 5. 崩溃在页 rename 之前：正式页不存在，tmp 被清理，重跑不漏不重
# --------------------------------------------------------------------- #
class CrashSimulated(Exception):
    pass


def test_crash_before_page_rename_resumes_without_dup_or_loss(
    dirs, tmp_path
):
    mount, work = dirs
    req = make_job("j5", mount, [[1, 4, 7], [2, 5, 8], [3, 6, 9]], page_size=4)

    calls = {"n": 0}

    def hook(point: str):
        if point == "after_page_tmp":
            calls["n"] += 1
            raise CrashSimulated(point)

    svc = MergeService(mount, work, crash_hook=hook)
    svc.register_job(req)
    with pytest.raises(CrashSimulated):
        svc.advance("j5")

    job_dir = work / "j5"
    assert not (job_dir / page_filename(1)).exists()   # 正式页未产生
    assert (job_dir / (page_filename(1) + ".tmp")).exists()
    assert calls["n"] == 1

    # 模拟进程重启：新实例、无 hook
    svc2 = MergeService(mount, work)
    result = svc2.run_to_end("j5")
    assert result.finished
    assert result.published == [1, 2, 3]               # 9 行 / 4 -> 3 页
    assert not list(job_dir.glob("*.tmp"))
    got = _all_published(work, "j5", 3)
    assert [r["key"] for r in got] == list(range(1, 10))  # 不漏不重且有序


# --------------------------------------------------------------------- #
# 6. 崩溃在页 rename 之后、state 提交之前：孤儿页被丢弃后重发
# --------------------------------------------------------------------- #
def test_crash_after_page_rename_before_state_republishes_orphan(dirs):
    mount, work = dirs
    req = make_job("j6", mount, [[1, 4, 7], [2, 5, 8], [3, 6, 9]], page_size=4)

    def hook(point: str):
        if point == "after_page_rename":
            raise CrashSimulated(point)

    svc = MergeService(mount, work, crash_hook=hook)
    svc.register_job(req)
    with pytest.raises(CrashSimulated):
        svc.advance("j6")

    job_dir = work / "j6"
    orphan = job_dir / page_filename(1)
    assert orphan.exists()                              # 页已发布但状态未提交

    svc2 = MergeService(mount, work)
    result = svc2.run_to_end("j6")
    assert result.published == [1, 2, 3]
    got = _all_published(work, "j6", 3)
    assert [r["key"] for r in got] == list(range(1, 10))
    # 孤儿页被重发覆盖，内容仍是完整 4 行
    assert _read_page(work, "j6", 1) and len(_read_page(work, "j6", 1)) == 4


# --------------------------------------------------------------------- #
# 7. 多次中断后继续：已发布页字节保持不变
# --------------------------------------------------------------------- #
def test_published_pages_immutable_across_restarts(dirs):
    mount, work = dirs
    req = make_job("j7", mount, [list(range(0, 12, 2)),
                                 list(range(1, 12, 2))], page_size=3)
    svc = MergeService(mount, work)
    svc.register_job(req)
    svc.advance("j7")
    svc.advance("j7")
    page1_before = (work / "j7" / page_filename(1)).read_bytes()
    page2_before = (work / "j7" / page_filename(2)).read_bytes()

    # “重启”：换新服务实例继续
    MergeService(mount, work).run_to_end("j7")
    assert (work / "j7" / page_filename(1)).read_bytes() == page1_before
    assert (work / "j7" / page_filename(2)).read_bytes() == page2_before
    status = MergeService(mount, work).get_status("j7")
    assert status.total_published_rows == 12
    assert all(s.reclaimed for s in status.segments)
    # 段文件全部回收
    assert not list((mount / "segments").glob("*.jsonl"))


# --------------------------------------------------------------------- #
# 8. 回收对账（两阶段提交）：
#    8a) 页 rename 后、状态提交前崩溃：重启后整批重发，不漏不重
#    8b) 段 unlink 后、reclaimed 标记提交前崩溃：重启补做回收对账
# --------------------------------------------------------------------- #
def test_reclaim_reconciled_after_restart(dirs):
    mount, work = dirs
    req = make_job("j8", mount, [[1], [2, 3, 4, 5, 6]], page_size=2)

    def hook(point: str):
        if point == "after_page_rename":
            raise CrashSimulated(point)

    svc = MergeService(mount, work, crash_hook=hook)
    svc.register_job(req)
    with pytest.raises(CrashSimulated):
        svc.advance("j8")
    seg0 = mount / "segments/run-0.jsonl"
    assert seg0.exists()                                # 崩溃在回收之前，段仍在

    # 8b) 新进程在“unlink 之后、reclaimed 提交之前”崩溃
    def hook2(point: str):
        if point == "after_reclaim_unlink":
            raise CrashSimulated(point)

    svc2 = MergeService(mount, work, crash_hook=hook2)
    with pytest.raises(CrashSimulated):
        svc2.advance("j8")
    assert not seg0.exists()                            # unlink 已生效
    # 此时状态文件里段0 为 exhausted=True、reclaimed=False（中间态）
    mid_status = MergeService(mount, work).get_status("j8")  # 新实例只读不推进
    seg0_state = next(s for s in mid_status.segments if s.path == "segments/run-0.jsonl")
    assert seg0_state.exhausted and not seg0_state.reclaimed

    # 再次重启：对账补做回收，整批继续直到完成
    svc3 = MergeService(mount, work)
    result = svc3.run_to_end("j8")
    assert result.finished
    got = [r["key"] for r in _all_published(work, "j8", result.status.published_pages)]
    assert got == [1, 2, 3, 4, 5, 6]
    assert all(s.reclaimed for s in result.status.segments)
    assert not list((mount / "segments").glob("*.jsonl"))


# --------------------------------------------------------------------- #
# 9. 登记校验：未排序 / 键非整数 / 缺文件 / 路序重复
# --------------------------------------------------------------------- #
def test_register_validation(service: MergeService, dirs):
    mount, work = dirs

    # 未排序
    write_segment(mount, "a.jsonl", [3, 1])
    req = JobCreate(job_id="bad1", key_field="key", page_size=2, segments=[
        {"path": "a.jsonl", "order": 0}])  # type: ignore[arg-type]
    with pytest.raises(EngineError, match="未排序"):
        service.register_job(req)

    # 键非整数
    p = mount / "b.jsonl"
    p.write_text(json.dumps({"key": "x"}) + "\n", encoding="utf-8")
    req = JobCreate(job_id="bad2", key_field="key", page_size=2, segments=[
        {"path": "b.jsonl", "order": 0}])  # type: ignore[arg-type]
    with pytest.raises(EngineError, match="不是整数"):
        service.register_job(req)

    # 缺文件
    req = JobCreate(job_id="bad3", key_field="key", page_size=2, segments=[
        {"path": "missing.jsonl", "order": 0}])  # type: ignore[arg-type]
    with pytest.raises(EngineError, match="不存在"):
        service.register_job(req)

    # 路序重复 / 目录穿越：pydantic 在构造 JobCreate 时即拒绝
    write_segment(mount, "c.jsonl", [1])
    write_segment(mount, "d.jsonl", [2])
    with pytest.raises(Exception):
        JobCreate(job_id="bad4", key_field="key", page_size=2, segments=[
            {"path": "c.jsonl", "order": 0},  # type: ignore[list-item]
            {"path": "d.jsonl", "order": 0},  # type: ignore[list-item]
        ])


def test_register_is_idempotent_and_conflicts(service: MergeService, dirs):
    mount, work = dirs
    req = make_job("j9", mount, [[1, 2]], page_size=2)
    s1 = service.register_job(req)
    s2 = service.register_job(req)
    assert s1 == s2

    conflict = req.model_copy(update={"page_size": 99})
    with pytest.raises(EngineError, match="不一致"):
        service.register_job(conflict)


def test_job_not_found(service: MergeService):
    with pytest.raises(JobNotFound):
        service.get_status("nope")


def test_path_traversal_rejected(service: MergeService, dirs):
    mount, work = dirs
    with pytest.raises(Exception):
        JobCreate(job_id="j10", key_field="key", page_size=2, segments=[
            {"path": "../escape.jsonl", "order": 0},  # type: ignore[list-item]
        ])
