"""API 级测试：登记、推进、单页读取、错误码、重启续跑。"""

from __future__ import annotations

import json
from pathlib import Path

from app.config import Settings
from app.main import create_app

from .conftest import make_job


def _req_body(mount: Path, page_size: int = 3) -> dict:
    req = make_job("api-demo", mount,
                   [[1, 5, 9], [2, 5, 8], [3, 5, 7]], page_size=page_size)
    return req.model_dump()


def test_full_flow_via_api(client, dirs):
    mount, work = dirs
    body = _req_body(mount)

    # /mount 能看到待登记的段
    listed = client.get("/mount").json()
    assert "segments/run-0.jsonl" in listed["jsonl_files"]

    r = client.post("/jobs", json=body)
    assert r.status_code == 201
    status = r.json()
    assert status["published_pages"] == 0 and status["finished"] is False

    # 重复登记幂等
    assert client.post("/jobs", json=body).status_code == 201

    # 逐页推进
    r1 = client.post("/jobs/api-demo/advance").json()
    assert r1["page_no"] == 1 and r1["rows"] == 3
    r2 = client.post("/jobs/api-demo/advance").json()
    assert r2["page_no"] == 2 and r2["rows"] == 3
    r3 = client.post("/jobs/api-demo/advance").json()
    assert r3["page_no"] == 3 and r3["rows"] == 3 and r3["finished"] is True
    r4 = client.post("/jobs/api-demo/advance").json()
    assert r4["page_no"] is None and r4["finished"] is True

    # 页面清单与单页读取（NDJSON / JSON 两种形态）
    pages = client.get("/jobs/api-demo/pages").json()
    assert [p["page_no"] for p in pages] == [1, 2, 3]
    assert [p["rows"] for p in pages] == [3, 3, 3]

    raw = client.get("/jobs/api-demo/pages/2")
    assert raw.status_code == 200
    assert raw.headers["content-type"].startswith("application/x-ndjson")
    assert raw.headers["x-page-rows"] == "3"
    nd = [json.loads(line) for line in raw.text.splitlines()]

    as_json = client.get("/jobs/api-demo/pages/2/json").json()
    assert as_json["records"] == nd

    # 整体顺序与同键路序
    all_records = []
    for no in (1, 2, 3):
        all_records += client.get(f"/jobs/api-demo/pages/{no}/json").json()["records"]
    keys = [r["key"] for r in all_records]
    assert keys == sorted(keys)
    assert [int(r["payload"].split("-")[0][1:]) for r in all_records if r["key"] == 5] == [0, 1, 2]

    # 段文件已回收
    assert not list((mount / "segments").glob("*.jsonl"))


def test_page_not_found_and_job_not_found(client, dirs):
    mount, _ = dirs
    client.post("/jobs", json=_req_body(mount))
    assert client.get("/jobs/ghost").status_code == 404
    assert client.get("/jobs/api-demo/pages/1").status_code == 400  # 尚未发布
    client.post("/jobs/api-demo/advance")
    assert client.get("/jobs/api-demo/pages/9").status_code == 400


def test_validation_error_shape(client, dirs):
    mount, _ = dirs
    bad = {"job_id": "x", "key_field": "key", "page_size": 0,
           "segments": [{"path": "a.jsonl", "order": 0}]}
    assert client.post("/jobs", json=bad).status_code == 422


def test_restart_resumes_within_same_mount_and_work(dirs):
    """模拟进程重启：用相同挂载/工作目录重新 create_app，已发布页保持。"""
    from fastapi.testclient import TestClient

    mount, work = dirs
    body = _req_body(mount, page_size=4)

    app1 = create_app(Settings(mount_dir=mount, work_dir=work))
    with TestClient(app1) as c1:
        c1.post("/jobs", json=body)
        r = c1.post("/jobs/api-demo/advance").json()
        assert r["page_no"] == 1 and r["rows"] == 4
        page1_bytes = (work / "api-demo" / "page-00001.jsonl").read_bytes()

    # 新进程：状态从磁盘恢复
    app2 = create_app(Settings(mount_dir=mount, work_dir=work))
    with TestClient(app2) as c2:
        before = c2.get("/jobs/api-demo").json()
        assert before["published_pages"] == 1
        run = c2.post("/jobs/api-demo/run").json()
        assert run["finished"] is True
        assert (work / "api-demo" / "page-00001.jsonl").read_bytes() == page1_bytes
        final = c2.get("/jobs/api-demo").json()
        all_keys = []
        for no in range(1, final["published_pages"] + 1):
            all_keys += [r["key"] for r in
                         c2.get(f"/jobs/api-demo/pages/{no}/json").json()["records"]]
        # 9 条记录，键序 1,2,3,5,5,5,7,8,9
        assert all_keys == [1, 2, 3, 5, 5, 5, 7, 8, 9]
        assert not list((mount / "segments").glob("*.jsonl"))
