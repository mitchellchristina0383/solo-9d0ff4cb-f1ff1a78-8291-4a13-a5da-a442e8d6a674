# 离线归并发布服务（FastAPI）

离线排序最后一道归并：挂载目录里的多路 JSONL 段（每路已按整数键排序），
按路序做 k 路归并，逐页发布结果；每路任意时刻只保留一条待比较记录；
同键按 **路序 → 路内原行序** 输出；每页发布后回收记录已全部发布的段文件；
进程中断后已发布页保持原样，重启续跑、补做回收，整批 **不漏不重**。

路数可以超过进程可用的输入文件句柄数：登记校验、单步推进和连续运行
共用一个额度（`MERGE_MAX_OPEN_INPUT_FILES`，由 Docker Compose 启动时
注入），超出额度的路按持久化字节偏移按需打开/关闭，输出顺序与续跑语义
不变。

## 目录结构

```
app/
  config.py            环境变量配置（挂载/工作目录、输入句柄额度）
  models.py            请求/响应模型与登记校验
  engine.py            归并引擎：堆归并、句柄额度与 LRU、原子发布、两阶段回收、崩溃对账
  main.py              FastAPI 应用与 HTTP 端点
sample/
  input/segments/      3 路本地样例段（含跨段同键 key=5/9、路内同键 key=12）
  input/wide/          24 路演示段（用于“路数超过额度”的 /docs 演示）
  job.demo.json        样例登记请求体（page_size=4）
  job.wide.json        24 路演示登记请求体（page_size=6）
scripts/
  generate_sample.py   生成 3 路样例
  generate_wide_sample.py  生成 24 路演示样例
  smoke_e2e.py         真实 uvicorn 进程端到端冒烟（含 kill -9 中断重启）
  smoke_wide.py        24 路 > 额度 4 的真实进程端到端（含 kill -9、并发推进）
tests/                 33 项 pytest（引擎/API/边界/崩溃注入/句柄额度）
Dockerfile
docker-compose.yml
requirements.txt
```

## 快速开始（Docker Compose）

```bash
# 1. 准备样例段（仓库已自带，可重新生成）
python scripts/generate_sample.py
python scripts/generate_wide_sample.py   # 24 路演示

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

### 在 /docs 演示：登记并推进“路数超过额度”的作业

Compose 默认额度 **4**，而 `sample/input/wide/` 有 **24 路**：

1. `GET /mount` → 确认列表里有 `wide/run-000.jsonl` … `wide/run-023.jsonl`。
2. `POST /jobs` → 请求体粘贴 `sample/job.wide.json`（24 个 segment，
   `order` 0…23，`page_size=6`）。登记校验按段逐个流式进行，只占 1 个
   句柄，24 路也能登记成功。
3. `GET /limits` → `max_open_input_files=4`、`open_input_files=0`
   （校验完即归还）。然后反复 `POST /jobs/wide/advance`，每次 6 行；
   过程中 `peak_open` 始终 `<= 4`，`evictions` 随页数增长，
   `reuse_hits` 增长（同一次推进连续读同一路复用句柄）。
4. 发布若干页后 `docker compose restart`（或 `kill -9` 容器内进程），
   再 `POST /jobs/wide/run`。已发布页字节不变，从断点继续到结束，
   整批 120 行按 key 升序、同 key 按路序输出。
5. `GET /jobs/wide` → `finished=true`，`segments[].reclaimed=true`；
   `sample/input/wide/*.jsonl` 已被删除。重新运行
   `python scripts/generate_wide_sample.py` 可再次演示。

额度可覆盖：`MERGE_MAX_OPEN_INPUT_FILES=1 docker compose up --build -d`
（1 个句柄也能跑完 24 路，只是淘汰更频繁）。

## 不用容器的本地运行

```bash
python -m venv .venv && .venv/bin/python -m pip install -r requirements.txt
python scripts/generate_sample.py
MERGE_MOUNT_DIR=$PWD/sample/input MERGE_WORK_DIR=$PWD/sample/work \
MERGE_MAX_OPEN_INPUT_FILES=4 \
  .venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

## HTTP 接口（/docs 可见完整 Schema）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/healthz` | 健康检查 |
| GET  | `/mount` | 列出挂载目录内的 JSONL，登记时照抄相对路径 |
| GET  | `/limits` | 输入句柄额度、实时占用、峰值/复用/淘汰/等待计数 |
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

- 每路从状态中记录的字节偏移按需读取，先读一条进最小堆；
- 堆元素排序键为 `(整数键 key, 路序 order, 路内行序 seq)`，同键天然按
  路序、再按路内原行序输出；
- 每弹出一条，只从同一路补读一条进堆——任意时刻每路在堆中至多一条，
  整段文件不进内存（测试 `test_heap_never_holds_more_than_one_record_per_run`
  会监控堆规模）。

### 2. 输入句柄额度：路数可以超过进程额度

`InputHandleLimiter` 是进程级、所有作业共享的句柄池（容量即
`MERGE_MAX_OPEN_INPUT_FILES`）：

- **登记校验**：逐段流式校验，任何时刻只占 1 个句柄；结束后归还。
- **单步推进 / 连续运行**：每次读取 `_next_entry` 短暂 pin 一个句柄，
  读完 release；句柄留在 LRU 缓存里，同一次推进连续读同一路直接复用
  （`reuse_hits` 可观测）。超过额度时淘汰最久未用且未被 pin 的句柄，
  下次按 `reader.pos`（持久化/内存中的字节偏移）重新打开继续。
- **慢盘隔离**：名额未满时真正的 `open()` 在全局锁之外进行，一个作业
  读盘卡住只占住它自己的名额；只要还有空余名额，其他作业照常推进。
  额度耗尽且全部句柄被 pin 时才会等待。
- **读错归还**：读取抛错时 `hold()` 关闭该句柄并归还名额；一次
  advance/run/登记失败时 `drop_job()` 把这个作业本次占用的名额全部归还。
- **不丢不重**：句柄关掉再开都按 `reader.pos` / 状态里的 `offset`
  定位；堆中预读记录仍持久化其起始偏移（`entry.offset`），不属于
  句柄状态。
- 段被回收后立刻丢弃该段缓存句柄，名额立即回到全局池。

### 3. 页是发布单元，原子提交

- 页先写 `page-00001.jsonl.tmp`，`fsync` 文件与目录后 `os.replace`
  原子改名；状态 `state.json` 走同样的 tmp + fsync + replace；
- 已发布的正式页永远不会被改写。崩溃只可能落在三个窗口，
  启动时 `_open_job` 逐一对账：
  1. **页 rename 前**：正式页不存在，tmp 直接清理，该页重发；
  2. **页 rename 后、state 提交前**：孤儿页号不在 `1..published_pages`
     内，删除后按状态重发；
  3. **state 提交后**：状态即事实，从记录的偏移继续。
- 读段出错导致本次推进失败时，状态未提交，新页也不可见（tmp 由下次
  对账清理）；重试从已提交的 `published_pages` 与偏移继续。

### 4. 回收的两阶段提交

段记录全部随页发布（`exhausted`）后的回收顺序：

1. 先原子提交状态（`exhausted=True`、偏移落盘，`reclaimed=False`）；
2. `unlink` 段文件；
3. 再提交 `reclaimed=True`。

这样“作业未消费完但段文件缺失”不可能由服务自身崩溃造成（出现即报
`EngineError`，通常意味着外部误删）。若崩在 2、3 之间，重启对账发现
`exhausted && !reclaimed` 会补删补标；已标 `reclaimed` 但文件还在则补删。

### 5. 续跑

状态只持久化各路 **下一条待比较记录的起始字节偏移**。注意堆中持有的
记录是预读出来的，存活路必须存堆条目里的 `entry.offset` 而不是文件
游标，否则重启会跳过该记录（该 bug 由测试先行捕获并修复）。

## 自测

```bash
.venv/bin/python -m pytest -q                      # 33 项单元/API/崩溃注入/额度测试
.venv/bin/python scripts/smoke_e2e.py             # 真实进程 + kill -9 端到端
.venv/bin/python scripts/smoke_wide.py            # 24 路 > 额度 4 + kill -9 + 并发
```

覆盖内容：

- 跨段同键按路序、路内同键按原行序；分页只有末页可短；
- 堆规模不超过路数（每路一条待比较记录）；
- 路数（24/12/40 路）远超额度（2/3/4）时：登记校验、逐页推进、重启
  续跑、段回收全部正确，`peak_open` 不超过额度；
- 同一次推进内连续读同一路复用句柄（`open_calls==1`、`reuse_hits>=19`）；
- 一个作业读盘/打开卡住时，另一作业在空余名额下照常完成；
- 多作业多线程并发推进，峰值句柄数不超过额度且各自不漏不重；
- 读段抛错：归还名额、未提交页不可见、重试从已提交进度继续；
- 段全部发布后才回收，未消费完的保留；
- 页 rename 前 / 后两种崩溃注入，重启后不漏不重；
- unlink 后、回收标记提交前崩溃，重启补做回收；
- 多次重启后已发布页字节不变；200 行数据逐页推进并穿插重启的全量校验；
- 空段、单段、页大小大于总行数、同键跨页、空行容忍、目录穿越与登记校验；
- 非法额度配置（0、非整数）在启动/构造时直接报错。

## 余项 / 约束

- **本环境没有 Docker/容器运行时**（`docker` 命令不存在、无 socket、
  用户命名空间不可用），因此镜像构建与 `docker compose up` 未在本机
  实跑。Dockerfile/compose 只做常规 uvicorn 封装；同等流程已用 venv +
  真实 uvicorn 进程验证（`smoke_e2e.py`、`smoke_wide.py`，含 kill -9
  与 24 路 > 额度 4）。`docker compose config` 的 YAML 结构已用
  PyYAML 解析核对，但未用 Docker 自身校验。
- 句柄额度是**单进程**语义：uvicorn 单 worker 下即全局额度；若改用
  多 worker，每个 worker 各自持有一份额度（总额度 = worker 数 × 配置值）。
- 状态与页文件面向单实例设计；多副本挂载同一卷不在支持范围
  （作业内有线程锁，但没有跨进程分布式锁）。
- 段文件登记时做一次全量流式排序校验，超大段的登记耗时与段大小线性相关。
- 输出页保留输入行的原始 JSON 文本（规范化行尾），不重排字段。
- 额度耗尽且所有句柄都被 pin 时，新读取会等待；这是有意的背压，
  `/limits` 的 `waits` 可观测。
