FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt /srv/requirements.txt
RUN pip install --no-cache-dir -r /srv/requirements.txt

COPY app /srv/app

# 挂载目录（输入段，可写以便回收）与工作目录（状态 + 已发布页）
ENV MERGE_MOUNT_DIR=/data/input \
    MERGE_WORK_DIR=/data/work
RUN mkdir -p /data/input /data/work

EXPOSE 8000

# 容器内不带 --reload；进程重启（docker compose restart / 重新 up）即触发续跑
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
