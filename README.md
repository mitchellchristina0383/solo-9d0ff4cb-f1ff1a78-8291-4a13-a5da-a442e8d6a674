# 离线归并发布服务（FastAPI）

离线排序最后一道归并：挂载目录里的多路 JSONL 段（每路已按整数键排序），
按路序做 k 路归并，逐页发布结果；每路任意时刻只保留一条待比较记录；
同键按 **路序 → 路内原行序** 输出；每页发布后回收记录已全部发布的段文件；
进程中断后已发布页保持原样，重启续跑、补做回收，整批 **不漏不重**。

## 目录结构

```
app/
  config.py            环境变量配置（MERGE_MOUNT_DIR / MERGE_WORK_DIR）
  models.py            请求/响应模型与登记校验
  engine.py            归并引擎：堆归并、原子发布、两阶段回收、崩溃对账
  main.py              FastAPI 应用与 HTTP 端点
sample/
  input/segments/      3 路本地样例段（含跨段同键 key=5/9、路内同键 key=12）
  job.demo.json        样例登记请求体（page_size=4）
scripts/
  generate_sample.py   生成样例
  smoke_e2e.py         真实 uvicorn 进程端到端冒烟（含 kill -9 中断重启）
tests/                 21 项 pytest（引擎/API/边界/崩溃注入）
Dockerfile
docker-compose.yml
requirements.txt
```

## 快速开始（Docker Compose）

```bash
# 1. 准备样例段（仓库已自带，可重新生成）
python scripts/generate_sample.py

# 2. 启动（./sample/input 挂为输入目录，命名卷保存状态与已发布页）
docker compose up --build -d

# 3. 打开交互文档
#    http://127.0.0.1:8000/docs

# 4. 登记作业（可直接贴 sample/job.demo.json 的内容）
curl -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  --data @sample/job.demo.json

# 5. 每次推进发布一页；发布后自动回收已消费完的段文件
curl -X POST http://127.0.0.1:8000/jobs/demo/advance

# 或一次跑完
curl -X POST http://127.0.0.1:8000/jobs/demo/run

# 6. 单页读取（NDJSON 流式；/json 后缀返回 JSON 数组）
curl http://127.0.0.1:8000/jobs/demo/pages/1
curl http://127.0.0.1:8000/jobs/demo/pages/1/json
```

段文件被回收后，`sample/input/segments/` 下对应文件会消失；重新运行
`python scripts/generate_sample.py` 可还原样例。作业状态与页文件在
`merge-work` 命名卷内，`docker compose down`（不加 `-v`）后重新 `up`
即可续跑。

## 不用容器的本地运行

```bash
python -m venv .venv && .venv/bin/python -m pip install -r requirements.txt
python scripts/generate_sample.py
MERGE_MOUNT_DIR=$PWD/sample/input MERGE_WORK_DIR=$PWD/sample/work \
  .venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

## HTTP 接口（/docs 可见完整 Schema）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/healthz` | 健康检查 |
| GET  | `/mount` | 列出挂载目录内的 JSONL，登记时照抄相对路径 |
| POST | `/jobs` | 登记作业：`job_id`、`key_field`、`page_size`、`segments[{path,order}]` |
| GET  | `/jobs` / `/jobs/{id}` | 作业列表 / 状态（偏移、耗尽、回收标记、页计数） |
| POST | `/jobs/{id}/advance` | 推进一轮：发布下一页并回收耗尽段；已结束时 `page_no=null` |
| POST | `/jobs/{id}/run?max_pages=` | 服务端连续推进到结束（可限页数） |
| GET  | `/jobs/{id}/pages` | 已发布页清单 |
| GET  | `/jobs/{id}/pages/{n}` | 单页读取，`application/x-ndjson` 流式输出 |
| GET  | `/jobs/{id}/pages/{n}/json` | 同页内容的 JSON 数组形式 |

分页规则：除末页外每页严格等于 `page_size`，只有末页可短。

## 实现要点

### 1. k 路归并，每路只留一条待比较记录

- 每路打开一个文件句柄并 `seek` 到状态中记录的字节偏移，先读一条进最小堆；
- 堆元素排序键为 `(整数键 key, 路序 order, 路内行序 seq)`，同键天然按
  路序、再按路内原行序输出；
- 每弹出一条，只从同一路补读一条进堆——任意时刻每路在堆中至多一条，
  整段文件不进内存（测试 `test_heap_never_holds_more_than_one_record_per_run`
  会监控堆规模）。

### 2. 页是发布单元，原子提交

- 页先写 `page-00001.jsonl.tmp`，`fsync` 文件与目录后 `os.replace`
  原子改名；状态 `state.json` 走同样的 tmp + fsync + replace；
- 已发布的正式页永远不会被改写。崩溃只可能落在三个窗口，
  启动时 `_open_job` 逐一对账：
  1. **页 rename 前**：正式页不存在，tmp 直接清理，该页重发；
  2. **页 rename 后、state 提交前**：孤儿页号不在 `1..published_pages`
     内，删除后按状态重发；
  3. **state 提交后**：状态即事实，从记录的偏移继续。

### 3. 回收的两阶段提交

段记录全部随页发布（`exhausted`）后的回收顺序：

1. 先原子提交状态（`exhausted=True`、偏移落盘，`reclaimed=False`）；
2. `unlink` 段文件；
3. 再提交 `reclaimed=True`。

这样“作业未消费完但段文件缺失”不可能由服务自身崩溃造成（出现即报
`EngineError`，通常意味着外部误删）。若崩在 2、3 之间，重启对账发现
`exhausted && !reclaimed` 会补删补标；已标 `reclaimed` 但文件还在则补删。

### 4. 续跑

状态只持久化各路 **下一条待比较记录的起始字节偏移**。注意堆中持有的
记录是预读出来的，存活路必须存堆条目里的 `entry.offset` 而不是
`f.tell()`，否则重启会跳过该记录（该 bug 由测试先行捕获并修复）。

## 自测

```bash
.venv/bin/python -m pytest -q                      # 21 项单元/API/崩溃注入测试
.venv/bin/python scripts/smoke_e2e.py             # 真实进程 + kill -9 端到端
```

覆盖内容：

- 跨段同键按路序、路内同键按原行序；分页只有末页可短；
- 堆规模不超过路数（每路一条待比较记录）；
- 段全部发布后才回收，未消费完的保留；
- 页 rename 前 / 后两种崩溃注入，重启后不漏不重；
- unlink 后、回收标记提交前崩溃，重启补做回收；
- 多次重启后已发布页字节不变；200 行数据逐页推进并穿插重启的全量校验；
- 空段、单段、页大小大于总行数、同键跨页、空行容忍、目录穿越与登记校验。

## 余项 / 约束

- 本环境未安装 Docker，镜像构建与 `docker compose up` 未在本机实跑，
  但 Dockerfile/compose 仅做常规 uvicorn 封装，同等流程已用 venv +
  uvicorn 真实进程（含 kill -9 重启）验证。
- 状态与页文件面向单实例设计；多副本挂载同一卷不在支持范围
  （作业内有线程锁，但没有跨进程分布式锁）。
- 段文件登记时做一次全量流式排序校验，超大段的登记耗时与段大小线性相关。
- 输出页保留输入行的原始 JSON 文本（规范化行尾），不重排字段。
