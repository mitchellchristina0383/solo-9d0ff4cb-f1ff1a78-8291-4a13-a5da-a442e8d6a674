"""请求 / 响应模型。"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field, field_validator

JOB_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$"


class SegmentSpec(BaseModel):
    """一路输入段：path 相对挂载目录；order 决定同键的路序。"""

    path: str = Field(..., min_length=1, description="相对挂载目录的 JSONL 段文件路径")
    order: int = Field(..., ge=0, description="路序，从 0 开始；同键按路序升序输出")


class JobCreate(BaseModel):
    """登记作业（POST /jobs）。"""

    job_id: str = Field(..., pattern=JOB_ID_PATTERN)
    key_field: str = Field(..., min_length=1, description="记录中整数排序键的字段名")
    page_size: int = Field(..., gt=0, description="每页行数；只有末页允许短于该值")
    segments: list[SegmentSpec] = Field(..., min_length=1)

    @field_validator("segments")
    @classmethod
    def _check_orders(cls, v: list[SegmentSpec]) -> list[SegmentSpec]:
        orders = [s.order for s in v]
        if len(set(orders)) != len(orders):
            raise ValueError("segment order 必须唯一")
        if sorted(orders) != list(range(len(orders))):
            raise ValueError("segment order 必须是从 0 开始的连续整数")
        paths = [s.path for s in v]
        if len(set(paths)) != len(paths):
            raise ValueError("segment path 不能重复")
        for s in v:
            p = s.path
            if p.startswith(("/", "\\")) or ".." in Path(p).parts:
                raise ValueError(f"非法段路径: {p}")
        return v


class SegmentState(BaseModel):
    path: str
    order: int
    offset: int = Field(0, description="下一条待处理记录在段文件中的字节偏移")
    exhausted: bool = False
    reclaimed: bool = False


class JobStatus(BaseModel):
    job_id: str
    key_field: str
    page_size: int
    segments: list[SegmentState]
    published_pages: int = Field(..., description="已发布页数（page_no 从 1 起）")
    total_published_rows: int
    finished: bool
    next_page_no: int
    page_counts: list[int] = Field(..., description="每页实际行数")


class AdvanceResult(BaseModel):
    page_no: int | None = Field(None, description="本次新发布的页号；无新页（已完成）时为 null")
    page_size: int
    rows: int = Field(..., description="本次发布的行数")
    finished: bool
    reclaimed: list[str] = Field(default_factory=list, description="本次回收的段文件相对路径")
    status: JobStatus


class RunResult(BaseModel):
    published: list[int] = Field(..., description="本次 run 新发布的页号列表")
    finished: bool
    status: JobStatus


class PageMeta(BaseModel):
    page_no: int
    rows: int
    filename: str
