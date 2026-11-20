#!/usr/bin/env python3
"""生成本地样例：3 路已按 key 排序的 JSONL，刻意制造跨段同键。

输出布局（样例作业 job_id=demo）::

    sample/input/segments/run-000.jsonl   路序 0
    sample/input/segments/run-001.jsonl   路序 1
    sample/input/segments/run-002.jsonl   路序 2
    sample/job.demo.json                  登记请求体（每页 4 行）

同键分布（验证同键按路序、路内行序输出）::
    key=5  路0 / 路1 / 路2 各一条
    key=9  路1 / 路2 各一条
    key=12 路0 内两条（验证路内行序）
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SEG_DIR = ROOT / "sample" / "input" / "segments"

# (key, payload)；每个文件内部 key 非降序
RUNS: dict[str, list[tuple[int, str]]] = {
    "run-000.jsonl": [
        (1, "alpha"), (3, "gamma"), (5, "epsilon-r0"),
        (8, "theta-r0"), (12, "mu-r0-a"), (12, "mu-r0-b"),
        (15, "xi-r0"),
    ],
    "run-001.jsonl": [
        (2, "beta-r1"), (5, "epsilon-r1"), (6, "zeta-r1"),
        (9, "iota-r1-a"), (9, "iota-r1-b"), (13, "nu-r1"),
    ],
    "run-002.jsonl": [
        (4, "delta-r2"), (5, "epsilon-r2"), (7, "eta-r2"),
        (9, "iota-r2"), (11, "lambda-r2"), (14, "xi-r2"),
    ],
}

PAGE_SIZE = 4
JOB_ID = "demo"
KEY_FIELD = "key"


def main() -> None:
    SEG_DIR.mkdir(parents=True, exist_ok=True)
    for name, rows in RUNS.items():
        keys = [k for k, _ in rows]
        assert keys == sorted(keys), f"{name} 未排序"
        path = SEG_DIR / name
        with open(path, "w", encoding="utf-8") as f:
            for i, (key, payload) in enumerate(rows):
                f.write(json.dumps(
                    {"key": key, "payload": payload, "source": name, "line": i},
                    ensure_ascii=False,
                ) + "\n")
        print(f"wrote {path.relative_to(ROOT)} ({len(rows)} rows)")

    job_req = {
        "job_id": JOB_ID,
        "key_field": KEY_FIELD,
        "page_size": PAGE_SIZE,
        "segments": [
            {"path": f"segments/{name}", "order": order}
            for order, name in enumerate(sorted(RUNS))
        ],
    }
    job_path = ROOT / "sample" / "job.demo.json"
    job_path.write_text(json.dumps(job_req, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8")
    total = sum(len(v) for v in RUNS.values())
    print(f"wrote {job_path.relative_to(ROOT)} (total {total} rows, page_size {PAGE_SIZE})")


if __name__ == "__main__":
    main()
