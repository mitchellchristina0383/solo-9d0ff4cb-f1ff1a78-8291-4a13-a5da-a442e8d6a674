"""FastAPI 归并服务。

端点
----
POST   /jobs                       登记作业（段文件、路序、每页行数）
GET    /jobs                       作业列表
GET    /jobs/{job_id}              作业状态
POST   /jobs/{job_id}/advance      推进并发布一页，回收耗尽的段文件
POST   /jobs/{job_id}/run          连续推进到结束（服务端循环）
GET    /jobs/{job_id}/pages        已发布页清单
GET    /jobs/{job_id}/pages/{no}   单页读取（NDJSON 流式）
GET    /jobs/{job_id}/pages/{no}/json  单页读取（JSON 数组）
GET    /mount                      查看挂载目录（便于在 /docs 对照登记）
GET    /limits                     输入句柄额度与实时占用
GET    /healthz                    健康检查
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

from .config import Settings
from .engine import EngineError, JobNotFound, MergeService
from .models import (
    AdvanceResult,
    JobCreate,
    JobStatus,
    PageMeta,
    RunResult,
)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    service = MergeService(
        mount_dir=settings.mount_dir,
        work_dir=settings.work_dir,
        max_open_input_files=settings.max_open_input_files,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> Any:
        # 启动即确保目录存在；状态恢复是惰性的，在首次访问作业时对账
        settings.mount_dir.mkdir(parents=True, exist_ok=True)
        settings.work_dir.mkdir(parents=True, exist_ok=True)
        yield

    app = FastAPI(
        title="离线归并发布服务",
        version="1.1.0",
        description=(
            "k 路已排序 JSONL 段的末道归并：逐页发布、单页读取、"
            "发布后回收段文件，支持进程中断后续跑，不漏不重。\n\n"
            "输入段文件句柄由进程级额度 `MERGE_MAX_OPEN_INPUT_FILES` "
            "统一约束：登记校验、`/advance`、`/run` 共用同一额度，"
            "**路数可以大于额度**。超过额度时按 LRU 打开/关闭段文件"
            "（按持久化偏移继续），输出顺序、断点续跑与段回收行为不变；"
            "某个作业读盘卡住时，只要还有空余名额，其他作业仍能推进。"
            "`GET /limits` 可查看额度与实时占用。\n\n"
            "### 演示：登记并推进路数超过额度的作业\n\n"
            "本仓库自带的 `sample/input/wide/` 有 **24 路**段，而 Compose "
            "默认额度是 **4**。在容器里：\n\n"
            "1. `GET /mount` 确认能看到 `wide/run-000.jsonl` … "
            "`wide/run-023.jsonl`；\n"
            "2. `POST /jobs`，请求体直接粘贴 `sample/job.wide.json`"
            "（24 个 segment，`page_size=6`）；\n"
            "3. `POST /jobs/wide/advance` 连续调用，每页 6 行；"
            "观察 `GET /limits` 中 `peak_open <= 4`、`evictions` 增长；\n"
            "4. 发布几页后 `docker compose restart`（或 kill -9），再 "
            "`POST /jobs/wide/run`：从已发布页之后继续，整批 120 行 "
            "按 key、同 key 按路序输出；\n"
            "5. `GET /jobs/wide` 查看 `segments[].reclaimed`，耗尽的段"
            "文件已从挂载目录删除。"
        ),
        lifespan=lifespan,
    )

    def _raise_engine(e: EngineError) -> HTTPException:
        if isinstance(e, JobNotFound):
            return HTTPException(status_code=404, detail=str(e))
        return HTTPException(status_code=400, detail=str(e))

    @app.get("/healthz", tags=["meta"])
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/mount", tags=["meta"])
    def show_mount() -> dict[str, Any]:
        """列出挂载目录下的 JSONL 文件（相对路径），登记作业时照抄即可。"""
        root = settings.mount_dir
        files: list[str] = []
        if root.exists():
            for p in sorted(root.rglob("*")):
                if p.is_file() and p.suffix == ".jsonl":
                    files.append(str(p.relative_to(root)))
        return {"mount_dir": str(root), "jsonl_files": files}

    @app.get("/limits", tags=["meta"], summary="输入句柄额度与实时占用")
    def show_limits() -> dict[str, Any]:
        """输入句柄额度与实时占用。

        返回 `max_open_input_files`（配置）、`open_input_files`（当前
        缓存中的句柄数）、`reserved`（正在打开/预留的名额）、`open_calls`
        （累计真正 open 次数）、`reuse_hits`（复用次数）、`evictions`
        （因额度满被淘汰的次数）、`peak_open`（峰值）、`waits`（因额度
        耗尽而等待的次数）。路数超过额度时 `peak_open` 不会超过上限。
        """
        stats = service.limiter.stats()
        return {
            "max_open_input_files": settings.max_open_input_files,
            "open_input_files": stats["open_now"],
            "reserved": stats["reserved"],
            "open_calls": stats["open_calls"],
            "reuse_hits": stats["reuse_hits"],
            "evictions": stats["evictions"],
            "peak_open": stats["peak_open"],
            "waits": stats["waits"],
        }

    @app.post("/jobs", response_model=JobStatus, status_code=201, tags=["jobs"])
    def register_job(req: JobCreate) -> JobStatus:
        try:
            return service.register_job(req)
        except EngineError as e:
            raise _raise_engine(e) from e

    @app.get("/jobs", response_model=list[JobStatus], tags=["jobs"])
    def list_jobs() -> list[JobStatus]:
        return service.list_jobs()

    @app.get("/jobs/{job_id}", response_model=JobStatus, tags=["jobs"])
    def get_job(job_id: str) -> JobStatus:
        try:
            return service.get_status(job_id)
        except EngineError as e:
            raise _raise_engine(e) from e

    @app.post("/jobs/{job_id}/advance", response_model=AdvanceResult, tags=["jobs"])
    def advance_job(job_id: str) -> AdvanceResult:
        """推进一轮：发布下一页（若有），并回收已全部发布的段文件。"""
        try:
            return service.advance(job_id)
        except EngineError as e:
            raise _raise_engine(e) from e

    @app.post("/jobs/{job_id}/run", response_model=RunResult, tags=["jobs"])
    def run_job(job_id: str, max_pages: int | None = None) -> RunResult:
        """服务端循环推进到结束；max_pages 用于分批驱动。"""
        try:
            return service.run_to_end(job_id, max_pages=max_pages)
        except EngineError as e:
            raise _raise_engine(e) from e

    @app.get("/jobs/{job_id}/pages", response_model=list[PageMeta], tags=["pages"])
    def list_pages(job_id: str) -> list[PageMeta]:
        try:
            return service.list_pages(job_id)
        except EngineError as e:
            raise _raise_engine(e) from e

    @app.get("/jobs/{job_id}/pages/{page_no}", tags=["pages"])
    def read_page(job_id: str, page_no: int, download: bool = False) -> StreamingResponse:
        """单页读取，返回 application/x-ndjson，逐行流式输出。"""
        try:
            meta, lines = service.read_page_lines(job_id, page_no)
        except EngineError as e:
            raise _raise_engine(e) from e

        def _stream() -> Any:
            for line in lines:
                yield line

        headers = {"X-Page-No": str(meta.page_no), "X-Page-Rows": str(meta.rows)}
        if download:
            headers["Content-Disposition"] = f'attachment; filename="{meta.filename}"'
        return StreamingResponse(
            _stream(),
            media_type="application/x-ndjson",
            headers=headers,
        )

    @app.get("/jobs/{job_id}/pages/{page_no}/json", tags=["pages"])
    def read_page_as_json(job_id: str, page_no: int) -> dict[str, Any]:
        """同页内容的 JSON 数组形式，方便浏览器 / 客户端直接消费。"""
        try:
            meta, lines = service.read_page_lines(job_id, page_no)
        except EngineError as e:
            raise _raise_engine(e) from e
        records = [json.loads(line) for line in lines]
        return {"page_no": meta.page_no, "rows": meta.rows, "records": records}

    # 暴露给测试使用
    app.state.service = service
    app.state.settings = settings
    return app


# 模块级默认应用，供 uvicorn app.main:app 使用
app = create_app()
