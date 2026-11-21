"""输入句柄额度测试：路数超过额度、校验/推进/连跑共享额度、复用、争用与读错重试。"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from app.config import Settings
from app.engine import InputHandleLimiter, MergeService, page_filename
from app.models import JobCreate, SegmentSpec

from .conftest import write_segment


def _job(job_id: str, mount: Path, n_runs: int, rows_per_run: int,
         page_size: int = 5, prefix: str = "segments") -> JobCreate:
    """构造 n_runs 路：第 i 路持有 i, i+n_runs, ...，保证键序交错。"""
    segs = []
    for i in range(n_runs):
        keys = list(range(i, rows_per_run * n_runs, n_runs))
        rel = f"{prefix}/run-{i:03d}.jsonl"
        write_segment(mount, rel, keys, extra=f"r{i}")
        segs.append(SegmentSpec(path=rel, order=i))
    return JobCreate(job_id=job_id, key_field="key", page_size=page_size, segments=segs)


def _all_records(work: Path, job_id: str, n_pages: int) -> list[dict]:
    out: list[dict] = []
    for no in range(1, n_pages + 1):
        path = work / job_id / page_filename(no)
        out.extend(json.loads(x) for x in path.read_text().splitlines() if x.strip())
    return out


# --------------------------------------------------------------------- #
# 1. 登记校验受同一额度约束：段数远超额度也只占额度内句柄
# --------------------------------------------------------------------- #
def test_registration_validation_respects_handle_limit(dirs):
    mount, work = dirs
    limit = 2
    req = _job("wide-reg", mount, n_runs=40, rows_per_run=3)

    svc = MergeService(mount, work, max_open_input_files=limit)
    status = svc.register_job(req)

    assert len(status.segments) == 40 > limit
    stats = svc.limiter.stats()
    assert stats["max_open"] == limit
    assert stats["peak_open"] <= limit, stats
    # 登记结束后本次校验占用的名额已归还
    assert svc.limiter.job_open_count("wide-reg") == 0
    assert stats["open_now"] == 0


# --------------------------------------------------------------------- #
# 2. 路数超过额度：逐页发布、重启续跑、按原顺序输出、回收耗尽的段
# --------------------------------------------------------------------- #
def test_wide_job_pages_resume_and_reclaim(dirs):
    mount, work = dirs
    limit = 3
    n_runs, rows_per_run, page_size = 12, 6, 4

    svc = MergeService(mount, work, max_open_input_files=limit)
    svc.register_job(_job("wide", mount, n_runs, rows_per_run, page_size=page_size))

    first = svc.advance("wide")
    assert first.page_no == 1 and first.rows == page_size
    second = svc.advance("wide")
    assert second.page_no == 2
    page1_before = (work / "wide" / page_filename(1)).read_bytes()

    # 模拟进程重启：同一工作目录的新实例共享额度，从磁盘状态继续
    svc2 = MergeService(mount, work, max_open_input_files=limit)
    result = svc2.run_to_end("wide")

    assert result.finished
    status = svc2.get_status("wide")
    assert status.published_pages == result.status.published_pages
    # 除末页外每页满页
    assert all(c == page_size for c in status.page_counts[:-1])
    assert status.page_counts[-1] <= page_size

    records = _all_records(work, "wide", status.published_pages)
    expected = sorted(
        (k, i) for i in range(n_runs) for k in range(i, rows_per_run * n_runs, n_runs)
    )
    # 键升序；同键按路序
    assert [(r["key"], int(r["payload"].split("-")[0][1:])) for r in records] == expected
    assert len(records) == n_runs * rows_per_run

    # 已发布页字节不变；所有段都回收
    assert (work / "wide" / page_filename(1)).read_bytes() == page1_before
    assert not list((mount / "segments").glob("*.jsonl"))
    assert all(s.reclaimed for s in status.segments)
    assert svc2.limiter.stats()["peak_open"] <= limit


# --------------------------------------------------------------------- #
# 3. 同一次推进内连续读同一路：复用已打开文件，不重复 open
# --------------------------------------------------------------------- #
def test_same_run_handle_reused_within_advance(dirs):
    mount, work = dirs
    req = _job("reuse", mount, n_runs=1, rows_per_run=20, page_size=20)
    svc = MergeService(mount, work, max_open_input_files=1)
    svc.register_job(req)
    before = svc.limiter.stats()
    assert before["open_now"] == 0  # 登记校验的句柄已归还
    calls_before = before["open_calls"]

    result = svc.advance("reuse")
    assert result.rows == 20
    stats = svc.limiter.stats()
    assert stats["open_calls"] - calls_before == 1  # 一次推进只真正 open 一次
    assert stats["reuse_hits"] >= 19    # 其余 19 次读取复用同一句柄
    assert stats["peak_open"] == 1


# --------------------------------------------------------------------- #
# 4. 一个作业读盘卡住但仍有空余名额：其他作业照常推进
# --------------------------------------------------------------------- #
def test_stuck_job_does_not_block_other_job_with_free_slot(dirs, monkeypatch):
    mount, work = dirs
    limit = 2
    svc = MergeService(mount, work, max_open_input_files=limit)
    svc.register_job(_job("stuck", mount, n_runs=2, rows_per_run=5, page_size=3,
                          prefix="stuck-segs"))
    svc.register_job(_job("other", mount, n_runs=1, rows_per_run=5, page_size=2,
                          prefix="other-segs"))
    shared = mount / "segments"
    if shared.exists() and not any(shared.iterdir()):
        shared.rmdir()

    started = threading.Event()
    release = threading.Event()
    stuck_file = (mount / "stuck-segs/run-000.jsonl").resolve()
    import builtins

    real_open = builtins.open
    blocked = {"done": False}

    def blocking_open(file, *args, **kwargs):
        if (not blocked["done"] and Path(file).resolve() == stuck_file):
            blocked["done"] = True
            # 模拟 open 卡在慢盘：名额已预留但拿不到句柄
            started.set()
            release.wait(timeout=10)
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", blocking_open)

    errors: list[BaseException] = []

    def run_stuck():
        try:
            svc.run_to_end("stuck")
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    t = threading.Thread(target=run_stuck)
    t.start()
    try:
        assert started.wait(timeout=5), "卡住作业未开始读"

        # 额度 2：卡住作业预留 1 个，另一个作业仍有空余名额，应能完成
        result = svc.run_to_end("other")
        assert result.finished and result.published
        assert svc.limiter.stats()["waits"] == 0
    finally:
        release.set()
        t.join(timeout=10)

    assert not errors, errors
    assert svc.get_status("stuck").finished


# --------------------------------------------------------------------- #
# 4b. 句柄级别：一个读取正被钉住时，另一个作业仍能拿到空余名额
# --------------------------------------------------------------------- #
def test_pinned_handle_does_not_consume_global_lock(tmp_path: Path):
    seg_a = tmp_path / "a.jsonl"
    seg_b = tmp_path / "b.jsonl"
    seg_a.write_bytes(b'{"key": 1}\n')
    seg_b.write_bytes(b'{"key": 2}\n')
    limiter = InputHandleLimiter(2)

    holder_started = threading.Event()
    release = threading.Event()

    def holder():
        with limiter.hold(("job-a", "read", 0), seg_a, 0) as f:
            holder_started.set()
            release.wait(timeout=10)
            f.readline()

    t = threading.Thread(target=holder)
    t.start()
    try:
        assert holder_started.wait(timeout=5)
        # 卡住的读取只钉住自己那 1 个名额；另一个作业仍可推进
        with limiter.hold(("job-b", "read", 0), seg_b, 0) as f:
            assert f.readline() == b'{"key": 2}\n'
        assert limiter.stats()["waits"] == 0
    finally:
        release.set()
        t.join(timeout=10)


# --------------------------------------------------------------------- #
# 4c. 多线程争用：跨作业并发推进，峰值打开数不超过额度
# --------------------------------------------------------------------- #
def test_concurrent_jobs_never_exceed_limit(dirs):
    mount, work = dirs
    limit = 3
    svc = MergeService(mount, work, max_open_input_files=limit)
    jobs = []
    for j in range(4):
        req = _job(f"conc-{j}", mount, n_runs=8, rows_per_run=5,
                   page_size=3, prefix=f"conc-{j}-segs")
        svc.register_job(req)
        jobs.append(req.job_id)

    errors: list[BaseException] = []

    def drive(job_id: str):
        local = MergeService(mount, work, max_open_input_files=limit)
        try:
            result = local.run_to_end(job_id)
            assert result.finished
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=drive, args=(j,)) for j in jobs]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors, errors
    stats = svc.limiter.stats()
    assert stats["peak_open"] <= limit, stats
    assert stats["open_now"] <= limit, stats
    for j in jobs:
        status = svc.get_status(j)
        assert status.finished and status.total_published_rows == 40
        got = [r["key"] for r in _all_records(work, j, status.published_pages)]
        assert got == sorted(got) and len(got) == 40


# --------------------------------------------------------------------- #
# 5. 读段出错：归还名额、未提交页不可见、重试从已提交进度继续
# --------------------------------------------------------------------- #
def test_read_error_returns_slots_and_retry_resumes_from_committed(dirs, monkeypatch):
    mount, work = dirs
    limit = 2
    svc = MergeService(mount, work, max_open_input_files=limit)
    svc.register_job(_job("boom", mount, n_runs=3, rows_per_run=8, page_size=4))

    first = svc.advance("boom")
    assert first.page_no == 1 and first.rows == 4
    page1 = (work / "boom" / page_filename(1)).read_bytes()
    committed = svc.get_status("boom")
    assert committed.published_pages == 1

    original = MergeService._next_entry
    calls = {"n": 0}

    def failing_next_entry(self, reader, key_field):
        calls["n"] += 1
        if calls["n"] == 3:
            # 先故意占一个名额，再模拟读盘 OSError，验证异常路径会归还
            with self.limiter.hold(reader.handle_key, reader.path, reader.pos):
                raise OSError("模拟读盘失败")
        return original(self, reader, key_field)

    monkeypatch.setattr(MergeService, "_next_entry", failing_next_entry)
    with pytest.raises(OSError, match="模拟读盘失败"):
        svc.advance("boom")

    # 本次请求占用的名额全部归还
    assert svc.limiter.job_open_count("boom") == 0
    assert svc.limiter.open_count == 0
    # 未提交的页不可见，状态仍停在上一次已提交的进度
    assert not (work / "boom" / page_filename(2)).exists()
    assert svc.get_status("boom").published_pages == 1
    assert svc.get_status("boom").total_published_rows == 4
    assert (work / "boom" / page_filename(1)).read_bytes() == page1

    # 故障消失后重试：从已提交进度继续，整批不漏不重
    monkeypatch.setattr(MergeService, "_next_entry", original)
    result = svc.run_to_end("boom")
    assert result.finished
    records = _all_records(work, "boom", result.status.published_pages)
    assert [r["key"] for r in records] == sorted(r["key"] for r in records)
    assert len(records) == 24
    assert (work / "boom" / page_filename(1)).read_bytes() == page1
    assert not list((mount / "segments").glob("*.jsonl"))


# --------------------------------------------------------------------- #
# 5b. 页已 rename、状态提交前出错：该页不算已发布，重试后按原内容发布
# --------------------------------------------------------------------- #
def test_page_not_visible_when_state_commit_fails(dirs, monkeypatch):
    mount, work = dirs
    svc = MergeService(mount, work, max_open_input_files=2)
    svc.register_job(_job("half", mount, n_runs=2, rows_per_run=5, page_size=3))

    real_write = MergeService._write_state
    calls = {"n": 0}

    def failing_write_state(self, job_id, status):
        calls["n"] += 1
        # 第 1 次是页面发布后的阶段1状态提交：模拟提交失败
        if calls["n"] == 1:
            raise OSError("模拟状态提交失败")
        return real_write(self, job_id, status)

    monkeypatch.setattr(MergeService, "_write_state", failing_write_state)
    with pytest.raises(OSError, match="模拟状态提交失败"):
        svc.advance("half")

    # 页文件可能已原子落盘，但状态未提交 → API 不可见
    assert svc.get_status("half").published_pages == 0
    assert svc.limiter.job_open_count("half") == 0

    # 重试：对账删除孤儿页后重新发布，整批不漏不重
    monkeypatch.setattr(MergeService, "_write_state", real_write)
    result = svc.run_to_end("half")
    assert result.finished
    records = _all_records(work, "half", result.status.published_pages)
    assert [r["key"] for r in records] == list(range(10))
    assert not list((mount / "segments").glob("*.jsonl"))


# --------------------------------------------------------------------- #
# 6. 额度配置与 /limits 可观测
# --------------------------------------------------------------------- #
def test_limits_endpoint_reports_configuration(dirs):
    from fastapi.testclient import TestClient

    from app.main import create_app

    mount, work = dirs
    app = create_app(Settings(
        mount_dir=mount, work_dir=work, max_open_input_files=2))
    with TestClient(app) as c:
        data = c.get("/limits").json()
        assert data["max_open_input_files"] == 2
        assert data["open_input_files"] == 0
        assert data["peak_open"] == 0


def test_settings_rejects_invalid_limit(monkeypatch):
    monkeypatch.setenv("MERGE_MAX_OPEN_INPUT_FILES", "0")
    with pytest.raises(ValueError, match="正整数"):
        Settings.from_env()
    monkeypatch.setenv("MERGE_MAX_OPEN_INPUT_FILES", "abc")
    with pytest.raises(ValueError, match="正整数"):
        Settings.from_env()
    monkeypatch.setenv("MERGE_MAX_OPEN_INPUT_FILES", "7")
    assert Settings.from_env().max_open_input_files == 7


def test_limiter_rejects_invalid_capacity():
    with pytest.raises(ValueError):
        InputHandleLimiter(0)
