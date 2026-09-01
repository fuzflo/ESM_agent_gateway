# ESM Gateway V1

V1 是一个 Railway 服务、一个对外 MCP endpoint：

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

## Notion 边界

- 真实四表作为空库启动，不使用虚拟或 sandbox 数据库。
- `esm_gateway/notion.py` 对四表所有字段、类型和冻结枚举执行只读 preflight。任何漂移阻止服务启动；代码不修改 schema。
- 未用字段保留为空，不删除、改名或改类型。
- Event 写入、Artifact 写入和 ExpertState 投影都做独立回读。
- 生产默认 `PROJECTION_READBACK_WAIT_SECONDS=0`：`confirm_write` 在 Event/所需 Artifact 持久化并回读后立即返回 `WRITE_COMMITTED_PROJECTION_PENDING`，不在 Web 请求内执行或等待 ExpertState 投影。Worker 完成后，后续 `get_expert_snapshot` 返回最新状态。
- 批量写入全部已提交但仍有投影待处理时，顶层返回 `BATCH_WRITE_COMMITTED_PROJECTION_PENDING`；只有每项投影回读都成功才返回 `BATCH_WRITE_COMPLETED`。失败子集用原 `batch_id` 和原 `temp_application_id` 安全重试。
- 只有 Event、所需 Artifact 和 ExpertState 都能一致回读时，上层 Agent 才能对 ESM 称为“完整成功”；`WRITE_COMMITTED_PROJECTION_PENDING` 不得说成写入失败，也不得重复提交 Event。
- Feishu 链接只作为人工存档地址，Gateway 不打开、不读取正文。

## 单副本约束

V1 Railway 必须配置为单副本。进程内锁保证同时 Event 写入的 `global_sequence` 和 stream version 串行；重启时从 Events 已有最高 sequence 恢复。V1 不支持多 Railway replica，也不支持同一 ESM 两个对话并发修改同一专家。

## 本地验证

```bash
python3 -m unittest discover -s ../tests_v1 -v
python3 scripts/preflight_notion.py
```

`preflight_notion.py` 只读 schema，不写数据。

## Railway

1. 以 `esm-gateway-v1` 为服务根目录。
2. 根据 `.env.example` 配置受保护变量。不需要、也不得添加“写入开关”。
3. 配置单 replica。
4. `Procfile` 启动 `python server.py`。
5. Leapility 只配置一个 MCP：`https://<railway-host>/mcp`，Bearer token 使用 `GATEWAY_TOKEN`。

日志全部输出 stdout。正常 Gateway started/completed 和 Uvicorn 启动信息为 info；身份拒绝为 info；可恢复合同/权限问题为 warning；provider 和真实未处理异常为 error。`Terminating session: None` 被过滤。
