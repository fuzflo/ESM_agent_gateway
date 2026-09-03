# ESM Gateway V1.1

V1.1 是 V1 的向后兼容合同修订版，保持一个 Railway 服务、一个对外 MCP endpoint：

```text
Leapility ESM Agent
        |
        v
    /mcp (Gateway)
        |
        +-- append-only Events / Artifact Index
        |
        +-- in-process Projection Worker
                    |
                    +-- ExpertState
                    +-- Projection Checkpoints
```

Gateway 和 Worker 在代码中是独立模块，但 V1 由同一进程运行。Worker 在服务启动时读取 checkpoint，每次 Event 写入后被立即唤醒；重启后从最后已提交的 `global_sequence` 继续。以后拆成两个服务时不需改 reducer 或存储合同。

## MCP 工具

```text
verify_esm_test_code
get_esm_roster              # Admin only
get_expert_snapshot
prepare_write               # validate + issue 10-minute card; no write
confirm_write               # append/readback; projection remains asynchronous
```

每个工具都拒绝未知外层参数；Gateway 服务层还会拒绝 command、candidate 和 Artifact 的未知字段。

`prepare_write` 必须在签发确认卡前验证将来真正写入的 Event payload，包括冻结的 `professional_domains` / `market_regions` 枚举与公开链接格式。已签发的确认卡不应因可预先检测的 payload 错误到 `confirm_write` 时才失败。

## 身份映射

10 位 ESM 共用一个 Railway 环境变量 `ESM_IDENTITY_MAP_JSON`，而不是 10 个环境变量。JSON 的 key 是每人当前唯一测试码，value 是：

```json
{
  "esm_owner_id": "ESM_01",
  "display_name": "ESM 01",
  "role": "esm"
}
```

启动时必须精确有 1 个 `admin` 和 9 个 `esm`，ID 必须唯一。单人同时只有一个有效测试码，V1 不支持新旧码重叠轮换。

## V1.1 枚举扩充

V1.1 在保留 V1 全部枚举的基础上新增：

```text
professional_domains: gtm | international_trade | cross_border_ecommerce
market_regions: china_mainland
```

部署前必须先在 Notion ExpertState 对应 multi-select 属性中增加这四个精确小写选项，再立即部署 Gateway V1.1。当前 V1 的 schema preflight 使用精确枚举集合；如果在添加 Notion 选项后重启旧 V1，它会因 schema 漂移拒绝启动，因此应在受控维护窗口连续完成 Notion 变更和 V1.1 部署。

## Notion 边界

- 真实四表作为空库启动，不使用虚拟或 sandbox 数据库。
- `esm_gateway/notion.py` 对四表所有字段、类型和冻结枚举执行只读 preflight。任何漂移阻止服务启动；代码不修改 schema。
- 未用字段保留为空，不删除、改名或改类型。
- Event 写入、Artifact 写入和 ExpertState 投影都做独立回读。
- 生产默认 `PROJECTION_READBACK_WAIT_SECONDS=0`：`confirm_write` 在 Event/所需 Artifact 持久化并回读后立即返回 `WRITE_COMMITTED_PROJECTION_PENDING`，不在 Web 请求内执行或等待 ExpertState 投影。Worker 完成后，后续 `get_expert_snapshot` 返回最新状态。
- 批量写入全部已提交但仍有投影待处理时，顶层返回 `BATCH_WRITE_COMMITTED_PROJECTION_PENDING`；只有每项投影回读都成功才返回 `BATCH_WRITE_COMPLETED`。失败子集用原 `batch_id` 和原 `temp_application_id` 安全重试。
- 只有 Event、所需 Artifact 和 ExpertState 都能一致回读时，上层 Agent 才能对 ESM 称为“完整成功”；`WRITE_COMMITTED_PROJECTION_PENDING` 不得说成写入失败，也不得重复提交 Event。
- Feishu 链接只作为人工存档地址，Gateway 不打开、不读取正文。

## Projection Worker 调度

- `PROJECTION_POLL_INTERVAL_SECONDS` 生产默认值为 `60`。这是无通知或进程外写入时的兜底轮询间隔，不是正常 Gateway 写入的投影延迟。
- Gateway 每次成功追加 Event 后调用 `projector.notify()`，同进程 Worker 会立即醒来，不等待下一次 60 秒轮询。
- Worker 启动后先立即执行一次扫描，再进入空闲等待；因此重启恢复已有积压 Event 也不等待 60 秒。
- Worker 在启动时从持久化 Checkpoint 读取一次当前位置，运行期间在内存中缓存已成功提交的位置。只有 `set_checkpoint` 成功后才推进内存缓存；失败时保持原位置并在后续扫描中安全重试。
- Worker 在 Checkpoint 读取、Event 扫描或单 Event 投影出现临时错误时不会终止后台 task。它记录错误、清除内存 Checkpoint 缓存，默认退避 5 秒后从持久化 Checkpoint 继续。
- Notion 只读请求的传输层失败默认最多尝试 3 次；写请求不盲目自动重试，仍依靠幂等 key 和回读确认。
- Event Writer 创建成功后，Gateway 使用返回的 Notion page ID 通过独立 Event Reader 直接回读，并以最长约 7 秒的退避窗口等待 Reader 可见；最后再以 `event_id` 索引查询兜底。
- `PROJECTION_RETRY_DELAY_SECONDS` 生产默认值为 `5`，用于 Worker 失败后的退避，避免 provider 故障时快速重试。
- 成功的底层 `httpx` 请求不写入 INFO 日志；Gateway 工具、Worker started/completed 和失败日志仍保留。需要临时排查 provider 请求时可单独提高 `httpx` logger 级别。
- `projection_worker_started` 会记录实际生效的轮询秒数和 batch size，部署后可据此确认 Railway 环境变量没有继续覆盖为旧值。
- `/healthz` 同时返回 Worker 是否存活、最后轮询/成功/失败时间、最后错误码和内存 Checkpoint。Worker 不存活时健康状态为 `degraded`。

## 单副本约束

V1.1 Railway 必须配置为单副本。进程内锁保证同时 Event 写入的 `global_sequence` 和 stream version 串行；重启时从 Events 已有最高 sequence 恢复。V1.1 不支持多 Railway replica，也不支持同一 ESM 两个对话并发修改同一专家。

## 本地验证

```bash
python3 -m unittest discover -s tests -v
python3 scripts/preflight_notion.py
```

`preflight_notion.py` 只读 schema，不写数据。

## Railway

1. 以 `esm-gateway-v1.1` 为服务根目录。
2. 根据 `.env.example` 配置受保护变量。不需要、也不得添加“写入开关”。
3. 配置单 replica。
4. `Procfile` 启动 `python server.py`。
5. Leapility 只配置一个 MCP：`https://<railway-host>/mcp`，Bearer token 使用 `GATEWAY_TOKEN`。

日志全部输出 stdout。正常 Gateway started/completed、Worker started/completed 和 Uvicorn 启动信息为 info；成功的 `httpx` provider 请求从 INFO 降噪，身份拒绝为 info；可恢复合同/权限问题为 warning；provider 和真实未处理异常为 error。`Terminating session: None` 被过滤。
