#!/usr/bin/env python3
"""端到端冒烟：拉起真实 uvicorn 进程，走完整 HTTP 流程，并验证进程中断续跑。

用法（在仓库根目录）::

    python scripts/smoke_e2e.py

脚本会：
  1. 在独立临时目录准备挂载段（含跨段同键）与工作目录；
  2. 启动 uvicorn 子进程，经 HTTP 登记作业、逐页推进、单页读取、核对回收；
  3. 发布若干页后直接 kill 子进程（模拟宕机），再启动新进程续跑；
  4. 校验已发布页字节不变、整批不漏不重、段文件全部回收。

不依赖第三方库，只用标准库，便于在精简环境里运行。
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
BASE = "http://127.0.0.1:8765"


def http(method: str, path: str, body: object = None) -> tuple[int, object]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            ctype = resp.headers.get("Content-Type", "")
            if ctype.split(";")[0].strip() == "application/json":
                return resp.status, json.loads(raw)
            return resp.status, raw
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def wait_ready(proc: subprocess.Popen) -> None:
    for _ in range(100):
        if proc.poll() is not None:
            raise RuntimeError("服务进程提前退出")
        try:
            code, _ = http("GET", "/healthz")
            if code == 200:
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError("服务未在 10s 内就绪")


def start_server(work: Path, mount: Path) -> subprocess.Popen:
    env = os.environ.copy()
    env["MERGE_MOUNT_DIR"] = str(mount)
    env["MERGE_WORK_DIR"] = str(work)
    env["PYTHONPATH"] = str(ROOT)
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", "8765", "--log-level", "warning"],
        cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    wait_ready(proc)
    return proc


def write_segments(mount: Path) -> list[list[int]]:
    seg_dir = mount / "segments"
    seg_dir.mkdir(parents=True)
    seg_keys = [
        [1, 5, 5, 9, 13, 13, 17],
        [2, 5, 7, 9, 11, 15],
        [3, 4, 5, 8, 9, 16],
    ]
    for order, keys in enumerate(seg_keys):
        with open(seg_dir / f"run-{order}.jsonl", "w", encoding="utf-8") as f:
            for i, k in enumerate(keys):
                f.write(json.dumps({"key": k, "run": order, "line": i}) + "\n")
    return seg_keys


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="merge-smoke-"))
    mount, work = tmp / "input", tmp / "work"
    mount.mkdir()
    work.mkdir()
    seg_keys = write_segments(mount)
    total_rows = sum(len(k) for k in seg_keys)
    expected_keys = sorted(k for keys in seg_keys for k in keys)

    job = {
        "job_id": "smoke",
        "key_field": "key",
        "page_size": 4,
        "segments": [
            {"path": f"segments/run-{i}.jsonl", "order": i} for i in range(3)
        ],
    }

    print(f"[1] 启动服务进程 #1（mount={mount}, work={work}）")
    proc = start_server(work, mount)
    try:
        code, listed = http("GET", "/mount")
        assert code == 200 and len(listed["jsonl_files"]) == 3, listed

        print("[2] POST /jobs 登记作业")
        code, status = http("POST", "/jobs", job)
        assert code == 201, status

        print("[3] 推进前两页")
        page1_before = None
        for expected_no in (1, 2):
            code, adv = http("POST", "/jobs/smoke/advance")
            assert code == 200 and adv["page_no"] == expected_no, adv
        code, pages = http("GET", "/jobs/smoke/pages")
        assert [p["page_no"] for p in pages] == [1, 2]

        # 单页 NDJSON 读取
        code, raw = http("GET", "/jobs/smoke/pages/1")
        assert code == 200
        page1_records = [json.loads(line) for line in raw.decode().splitlines()]
        assert len(page1_records) == 4
        page1_before = (work / "smoke" / "page-00001.jsonl").read_bytes()

        print("[4] kill -9 服务进程（模拟宕机，未走优雅退出）")
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=10)
    finally:
        if proc.poll() is None:
            proc.kill()

    print("[5] 启动服务进程 #2，验证续跑")
    proc2 = start_server(work, mount)
    try:
        code, status = http("GET", "/jobs/smoke")
        assert code == 200 and status["published_pages"] == 2, status
        # 已发布页字节保持原样
        assert (work / "smoke" / "page-00001.jsonl").read_bytes() == page1_before

        print("[6] /run 跑完剩余页")
        code, run = http("POST", "/jobs/smoke/run")
        assert code == 200 and run["finished"] is True, run

        all_keys: list[int] = []
        code, pages = http("GET", "/jobs/smoke/pages")
        for meta in pages:
            code, raw = http("GET", f"/jobs/smoke/pages/{meta['page_no']}")
            all_keys.extend(json.loads(line)["key"]
                            for line in raw.decode().splitlines())

        print("[7] 校验")
        assert all_keys == expected_keys, "整批顺序/完整性不符"
        assert len(all_keys) == total_rows and len(set(all_keys)) <= total_rows
        # 跨段同键顺序：key=5 的四条依次来自路 0,0,1,2
        key5_runs = []
        for meta in pages:
            code, raw = http("GET", f"/jobs/smoke/pages/{meta['page_no']}")
            for line in raw.decode().splitlines():
                rec = json.loads(line)
                if rec["key"] == 5:
                    key5_runs.append(rec["run"])
        assert key5_runs == [0, 0, 1, 2], key5_runs

        # 只有末页允许短
        counts = [p["rows"] for p in pages]
        assert all(c == 4 for c in counts[:-1]) and counts[-1] <= 4, counts

        # 段文件全部回收
        leftover = list((mount / "segments").glob("*.jsonl"))
        assert not leftover, leftover

        code, status = http("GET", "/jobs/smoke")
        assert status["finished"] and all(s["reclaimed"] for s in status["segments"])
    finally:
        proc2.send_signal(signal.SIGTERM)
        try:
            proc2.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc2.kill()

    print("OK：跨段同键顺序正确；kill -9 后续跑不漏不重；已发布页不变；段已回收")
    print(f"    总记录 {total_rows} 行，发布 {len(pages)} 页，页行数 {counts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
