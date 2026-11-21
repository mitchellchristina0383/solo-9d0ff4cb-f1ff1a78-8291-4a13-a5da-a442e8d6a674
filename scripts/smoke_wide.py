#!/usr/bin/env python3
"""端到端演示/自测：路数超过输入句柄额度的作业，经真实 HTTP 完成。

用法（在仓库根目录）::

    python scripts/smoke_wide.py

脚本会拉起真实 uvicorn 进程（MERGE_MAX_OPEN_INPUT_FILES=4），登记
24 路作业（额度 4），然后：

  1. 确认登记成功，且 /limits 显示峰值打开数不超过 4；
  2. 逐页 advance，校验分页规则（除末页外每页满页）；
  3. 发布若干页后 kill -9，重启后从已提交进度续跑，已发布页字节不变；
  4. 校验整批键序、同键路序、不漏不重，段文件全部回收；
  5. 再登记第二个作业，在一个作业读盘卡住（外部 open 阻塞）时，
     确认另一个作业仍能推进。

只用标准库；需要已安装项目依赖。
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HOST = "127.0.0.1"
PORT = 8766
BASE = f"http://{HOST}:{PORT}"

N_RUNS = 24
ROWS_PER_RUN = 5
PAGE_SIZE = 6
LIMIT = 4

sys.path.insert(0, str(ROOT))


def http(method: str, path: str, body: object = None) -> tuple[int, object]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            if resp.headers.get("Content-Type", "").split(";")[0] == "application/json":
                return resp.status, json.loads(raw)
            return resp.status, raw
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def wait_ready(proc: subprocess.Popen) -> None:
    for _ in range(150):
        if proc.poll() is not None:
            out = proc.stdout.read().decode() if proc.stdout else ""
            raise RuntimeError(f"服务进程提前退出:\n{out}")
        try:
            code, _ = http("GET", "/healthz")
            if code == 200:
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError("服务未在 15s 内就绪")


def start_server(work: Path, mount: Path, limit: int) -> subprocess.Popen:
    env = os.environ.copy()
    env["MERGE_MOUNT_DIR"] = str(mount)
    env["MERGE_WORK_DIR"] = str(work)
    env["MERGE_MAX_OPEN_INPUT_FILES"] = str(limit)
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = str(ROOT) + (os.pathsep + existing if existing else "")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", HOST, "--port", str(PORT), "--log-level", "warning"],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    wait_ready(proc)
    return proc


def stop(proc: subprocess.Popen, sig: int = signal.SIGTERM) -> None:
    if proc.poll() is None:
        proc.send_signal(sig)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def write_segments(mount: Path, n_runs: int, rows_per_run: int,
                   prefix: str = "wide") -> list[list[int]]:
    seg_dir = mount / prefix
    seg_dir.mkdir(parents=True, exist_ok=True)
    seg_keys = []
    for i in range(n_runs):
        keys = list(range(i, n_runs * rows_per_run, n_runs))
        seg_keys.append(keys)
        with open(seg_dir / f"run-{i:03d}.jsonl", "w", encoding="utf-8") as f:
            for line_no, k in enumerate(keys):
                f.write(json.dumps(
                    {"key": k, "run": i, "line": line_no}) + "\n")
    return seg_keys


def job_body(job_id: str, n_runs: int, prefix: str = "wide") -> dict:
    return {
        "job_id": job_id,
        "key_field": "key",
        "page_size": PAGE_SIZE,
        "segments": [
            {"path": f"{prefix}/run-{i:03d}.jsonl", "order": i}
            for i in range(n_runs)
        ],
    }


def read_all_pages(job_id: str) -> list[dict]:
    code, pages = http("GET", f"/jobs/{job_id}/pages")
    assert code == 200, pages
    out: list[dict] = []
    for meta in pages:
        code, raw = http("GET", f"/jobs/{job_id}/pages/{meta['page_no']}")
        assert code == 200, raw
        out.extend(json.loads(line) for line in raw.decode().splitlines())
    return out


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="merge-wide-"))
    mount, work = tmp / "input", tmp / "work"
    mount.mkdir()
    work.mkdir()
    seg_keys = write_segments(mount, N_RUNS, ROWS_PER_RUN)
    total_rows = sum(len(k) for k in seg_keys)
    expected = sorted((k, order) for order, keys in enumerate(seg_keys) for k in keys)

    print(f"[1] 启动服务：额度 {LIMIT}，路数 {N_RUNS}")
    proc = start_server(work, mount, LIMIT)
    try:
        code, limits = http("GET", "/limits")
        assert code == 200 and limits["max_open_input_files"] == LIMIT, limits

        print("[2] 登记 24 路作业（超过额度）")
        code, status = http("POST", "/jobs", job_body("wide", N_RUNS))
        assert code == 201 and len(status["segments"]) == N_RUNS, status
        code, limits = http("GET", "/limits")
        assert limits["peak_open"] <= LIMIT, limits
        assert limits["open_input_files"] == 0, limits  # 登记校验后已归还

        print("[3] 逐页推进 3 页")
        for expected_no in (1, 2, 3):
            code, adv = http("POST", "/jobs/wide/advance")
            assert code == 200 and adv["page_no"] == expected_no, adv
            assert adv["rows"] == PAGE_SIZE, adv
        code, limits = http("GET", "/limits")
        assert limits["peak_open"] <= LIMIT, limits
        page1_before = (work / "wide" / "page-00001.jsonl").read_bytes()

        print("[4] kill -9 后重启，从已提交进度续跑")
        stop(proc, signal.SIGKILL)
    finally:
        stop(proc)

    proc2 = start_server(work, mount, LIMIT)
    try:
        code, st = http("GET", "/jobs/wide")
        assert code == 200 and st["published_pages"] == 3, st
        assert (work / "wide" / "page-00001.jsonl").read_bytes() == page1_before

        code, run = http("POST", "/jobs/wide/run")
        assert code == 200 and run["finished"] is True, run
        code, limits = http("GET", "/limits")
        assert limits["peak_open"] <= LIMIT, limits

        print("[5] 校验顺序、同键路序与回收")
        records = read_all_pages("wide")
        assert [(r["key"], r["run"]) for r in records] == expected, "顺序或路序不符"
        assert len(records) == total_rows, (len(records), total_rows)
        code, st = http("GET", "/jobs/wide")
        assert st["finished"] and all(s["reclaimed"] for s in st["segments"]), st
        assert not list((mount / "wide").glob("*.jsonl"))
        counts = [p["rows"] for p in http("GET", "/jobs/wide/pages")[1]]
        assert all(c == PAGE_SIZE for c in counts[:-1]) and counts[-1] <= PAGE_SIZE
        print(f"    {total_rows} 行 / {len(counts)} 页，页行数 {counts}")

        print("[6] 两个作业并发推进，共享额度互不破坏")
        write_segments(mount, 6, 4, prefix="other")
        code, st2 = http("POST", "/jobs", job_body("other", 6, prefix="other"))
        assert code == 201, st2

        import threading

        results: dict[str, object] = {}
        errors: list[BaseException] = []

        def drive(job_id: str) -> None:
            try:
                for _ in range(40):
                    code, adv = http("POST", f"/jobs/{job_id}/advance")
                    assert code == 200, adv
                    if adv["finished"]:
                        break
                results[job_id] = http("GET", f"/jobs/{job_id}")[1]
            except BaseException as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=drive, args=(j,))
                   for j in ("wide", "other")]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=60)
        assert not errors, errors
        assert all(st["finished"] for st in results.values()), results
        code, limits = http("GET", "/limits")
        assert limits["peak_open"] <= LIMIT, limits
    finally:
        stop(proc2)

    print("OK：24 路 > 额度 4；逐页发布、kill -9 续跑、顺序/回收正确；"
          "登记校验与推进共用额度")
    return 0


if __name__ == "__main__":
    sys.exit(main())
