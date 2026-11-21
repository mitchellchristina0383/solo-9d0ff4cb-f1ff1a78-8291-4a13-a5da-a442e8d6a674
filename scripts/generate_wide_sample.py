#!/usr/bin/env python3
"""生成“路数超过句柄额度”的演示作业。

输出::

    sample/input/wide/run-000.jsonl ... run-023.jsonl   24 路，每路已排序
    sample/job.wide.json                                登记请求体

24 路 > Docker Compose 默认额度（MERGE_MAX_OPEN_INPUT_FILES=4），
用于在 /docs 演示：登记、逐页推进、单页读取、断点续跑与段回收。

重新生成会覆盖段文件；若服务已经推进并回收了部分段，需要先重新生成
再登记（作业 id 为 ``wide``，重复登记相同参数是幂等的）。
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SEG_DIR = ROOT / "sample" / "input" / "wide"
JOB_PATH = ROOT / "sample" / "job.wide.json"

N_RUNS = 24
ROWS_PER_RUN = 5
PAGE_SIZE = 6
JOB_ID = "wide"
KEY_FIELD = "key"


def main() -> None:
    SEG_DIR.mkdir(parents=True, exist_ok=True)
    # 第 i 路持有 i, i+N_RUNS, i+2*N_RUNS, ...，路内升序、路间交错
    for i in range(N_RUNS):
        keys = list(range(i, N_RUNS * ROWS_PER_RUN, N_RUNS))
        path = SEG_DIR / f"run-{i:03d}.jsonl"
        with open(path, "w", encoding="utf-8") as f:
            for line_no, key in enumerate(keys):
                f.write(json.dumps(
                    {
                        "key": key,
                        "payload": f"r{i:02d}-line{line_no}",
                        "run": i,
                        "line": line_no,
                    },
                    ensure_ascii=False,
                ) + "\n")

    job_req = {
        "job_id": JOB_ID,
        "key_field": KEY_FIELD,
        "page_size": PAGE_SIZE,
        "segments": [
            {"path": f"wide/run-{i:03d}.jsonl", "order": i}
            for i in range(N_RUNS)
        ],
    }
    JOB_PATH.write_text(
        json.dumps(job_req, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    total = N_RUNS * ROWS_PER_RUN
    pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    print(f"wrote {N_RUNS} segments under {SEG_DIR.relative_to(ROOT)}")
    print(f"wrote {JOB_PATH.relative_to(ROOT)}")
    print(f"total {total} rows, page_size {PAGE_SIZE} -> {pages} pages")


if __name__ == "__main__":
    main()
