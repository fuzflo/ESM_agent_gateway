# ESM Gateway V1.2 — CSV 第一阶段

本地验证：2026-10-13，`python3 -m unittest discover -s tests -q` 共 89 个测试通过（含真实 MCP 工具注册与闭合参数合同测试、20 个新增 CSV 用例）。未调用生产服务。

本目录复制自 V1.1.10，V1.1 保留。本阶段实现 Admin CSV 校验、预览与确认导入，仍为本地开发版本，尚未上线。新增 MCP 工具 `validate_csv_import` 和 `get_csv_import_status`；`prepare_write` 新增 `import_experts_csv_batch`，具体调用见 [CSV 合同](../esm-agent-v1.2/skills/esm-gateway-contract/references/csv-import.md)。

已实现 12 列解析、字段／Owner 校验、历史及文件内重复检查、同名确认、完整创建 Event 和两类资产、独立回读核验、异步投影与文字回执。受控失败行重试、Owner 查询扩展／首次排期确认、回执 CSV 下载尚未实现；平台附件能力和生产 Notion 写入尚未验收。完整阶段边界见 [编码进度](../esm-agent-v1.2/operations/V1.2-编码进度.md)。

上线前需要在维护窗口使 Notion 精确枚举与本目录 `notion.py` 匹配，包括 source 新选项、awaiting_progress_assessment、leapility_card_link、checkpoint 的 1.2.0 支持版本。没有自动修改 schema 的代码；旧 V1.1 直接回退可能无法通过新增 schema，回退构建尚待准备。

以下为继承的 V1.1 运维说明；历史版本说明保留用于说明兼容行为，部署时服务根目录使用 esm-gateway-v1.2。

# ESM Gateway V1.1

V1.1 是 V1 的向后兼容合同修订版，保持一个 Railway 服务、一个对外 MCP endpoint：

当前补丁版本为 V1.1.10。Notion 的可编辑 date 属性在实际回读中将 Artifact `reviewed_at` 降为分钟精度；本版本在确认卡签发前就将该字段规范为 UTC 分钟，并以相同精度做 Artifact 回读比较。同一分钟内的 provider 截断不再阻断 Event，不同分钟、仅日期值及其他字段差异仍拒绝。V1.1.9 的 `created_at` provider-normalized 规则、枚举扩充，以及 V1.1.8 的分阶段 Artifact/Event 写入回执保持不变。

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
confirm_write               # append with Writer acknowledgement; projection remains asynchronous
```

每个工具都拒绝未知外层参数；Gateway 服务层还会拒绝 command、candidate 和 Artifact 的未知字段。

`prepare_write` 必须在签发确认卡前验证将来真正写入的 Event payload，包括冻结的 `professional_domains` / `market_regions` 枚举与公开链接格式。已签发的确认卡不应因可预先检测的 payload 错误到 `confirm_write` 时才失败。

V1.1.6 起的 `confirmation_token` 是 43 字符随机 capability，不再包含 command 正文。Gateway 仅在内存中保存 token 的 HMAC-SHA-256 值和完整 claims；日志只记录 token 长度、hash 前 12 位和非敏感 confirmation ID，不记录原始 token 或 command 正文。对外 MCP 工具形状不变。

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

V1.1.9 追加的 Notion 精确选项为：

```text
professional_domains: professional_services | financial_services | retail_ecommerce |
  manufacturing_industrial | real_estate | construction_infrastructure |
  transportation_logistics | hospitality_travel | media_entertainment |
  energy_natural_resources | agriculture_food | life_sciences |
  government_public_policy | nonprofit_social_impact
market_regions: europe | middle_east | africa | greater_china
```

`emea` 不删除，以保证旧页面和历史 Event 仍可回读；Agent 的新写入必须改用 `europe` / `middle_east` / `africa`。`china_mainland` 继续表示仅中国大陆，`greater_china` 表示大中华区。完整中英文映射见 `esm-agent-v1.1/skills/esm-gateway-contract/references/enums.md`。由于 preflight 要求选项集合精确匹配，上线时仍要在同一维护窗口先改 Notion schema，再部署 V1.1.9。

## Notion 边界

- 真实四表作为空库启动，不使用虚拟或 sandbox 数据库。
- `esm_gateway/notion.py` 对四表所有字段、类型和冻结枚举执行只读 preflight。任何漂移阻止服务启动；代码不修改 schema。
- 未用字段保留为空，不删除、改名或改类型。
- Artifact 与 Event 写入都以对应 Notion Writer 的 2xx 创建响应和非空 page ID 作为 commit ack。Notion Reader 可能延迟可见；Artifact 创建获得 ack 后即继续追加 Event，不再用 Artifact 的即时回读作为 Event 写入前置条件。
- Artifact 回读把 `created_at` 作为 provider-normalized 审计元数据：写入值和回读值只要各自是非空、合法的 ISO-8601 日期/时间，就不比较具体值。`reviewed_at` 使用 Notion 保留的 UTC 分钟精度严格比较：同分钟内的秒/毫秒截断等价，不同分钟或仅日期值仍报 mismatch。check/retention 等其他业务或策略时间继续做 UTC 归一化后严格比较；`language` / `allowed_purposes` 做无序比较；链接、content hash、Artifact ID、logical ID、版本、Owner 和状态等字段仍严格相等。真实 mismatch 的日志只记录字段名，不记录字段值。
- 生产默认 `PROJECTION_READBACK_WAIT_SECONDS=90`：`confirm_write` 在 commit ack 后并行等待 Artifact 独立回读与 ExpertState 投影。两者都完成时返回 `WRITE_COMPLETED`；Artifact 仍不可见时返回 `WRITE_COMMITTED_READBACK_PENDING`；仅投影未完成时返回 `WRITE_COMMITTED_PROJECTION_PENDING`。
- 批量写入全部已提交但仍有投影待处理时，顶层返回 `BATCH_WRITE_COMMITTED_PROJECTION_PENDING`；只有每项投影回读都成功才返回 `BATCH_WRITE_COMPLETED`。失败子集用原 `batch_id` 和原 `temp_application_id` 安全重试。
- 业务写入结果显式返回 `artifact_commit_acknowledged`、`artifact_readback_succeeded`、`event_commit_acknowledged`、`projection_readback_succeeded` 和 `pending_phases`。存储错误额外返回 `failed_phase`；Agent 不得用 snapshot 中的 `artifact_versions` 推断 Event 已提交。
- 只有 Event、所需 Artifact 和 ExpertState 都能一致回读时，上层 Agent 才能对 ESM 称为“完整成功”；任何 `WRITE_COMMITTED_*` 不得说成写入失败，也不得重复提交 Event。
- Feishu 链接只作为人工存档地址，Gateway 不打开、不读取正文。

## Projection Worker 调度

- `PROJECTION_POLL_INTERVAL_SECONDS` 生产默认值为 `60`。这是无通知或进程外写入时的兜底轮询间隔，不是正常 Gateway 写入的投影延迟。
- Gateway 每次成功追加 Event 后调用 `projector.notify()`，同进程 Worker 会立即醒来，不等待下一次 60 秒轮询。
- Worker 启动后先立即执行一次扫描，再进入空闲等待；因此重启恢复已有积压 Event 也不等待 60 秒。
- Worker 在启动时从持久化 Checkpoint 读取一次当前位置，运行期间在内存中缓存已成功提交的位置。只有 `set_checkpoint` 成功后才推进内存缓存；失败时保持原位置并在后续扫描中安全重试。
- Worker 在 Checkpoint 读取、Event 扫描或单 Event 投影出现临时错误时不会终止后台 task。它记录错误、清除内存 Checkpoint 缓存，默认退避 5 秒后从持久化 Checkpoint 继续。
- Notion 只读请求的传输层失败默认最多尝试 3 次；写请求不盲目自动重试，仍依靠幂等 key 和回读确认。
- Event Writer 创建成功后，Gateway 验证响应中的 Notion page ID，然后立即唤醒 Worker。独立 Event Reader 的可见性延迟由 Worker 退避重试处理，不再将已提交 Event 返回为 `EVENT_READBACK_FAILED` 或 `EVENT_WRITE_ACK_INVALID`。
- `PROJECTION_RETRY_DELAY_SECONDS` 生产默认值为 `5`，用于 Worker 失败后的退避，避免 provider 故障时快速重试。
- 成功的底层 `httpx` 请求不写入 INFO 日志；Gateway 工具、Worker started/completed 和失败日志仍保留。需要临时排查 provider 请求时可单独提高 `httpx` logger 级别。
- `projection_worker_started` 会记录实际生效的轮询秒数和 batch size，部署后可据此确认 Railway 环境变量没有继续覆盖为旧值。
- `/healthz` 同时返回 Worker 是否存活、最后轮询/成功/失败时间、最后错误码和内存 Checkpoint。Worker 不存活时健康状态为 `degraded`。

## 单副本约束

V1.1 Railway 必须配置为单副本。进程内锁保证同时 Event 写入的 `global_sequence` 和 stream version 串行；重启时从 Events 已有最高 sequence 恢复。确认卡也存在当前 Gateway 进程内，因此 Railway 重启后旧卡失效。V1.1 不支持多 Railway replica，也不支持同一 ESM 两个对话并发修改同一专家。

## 本地验证

```bash
python3 -m unittest discover -s tests -v
python3 scripts/preflight_notion.py
```

`preflight_notion.py` 只读 schema，不写数据。

修复历史故障遗留的孤立 Artifact 时，先运行默认 dry-run：

```bash
python3 scripts/reconcile_orphan_artifact.py \
  --artifact-id ART-f0455701-1603-4991-93bb-7cffc5ee0bac \
  --temp-application-id TMP-E788E384-AA85-4AA8-BC9D-181CD1902D9E
```

只有 dry-run 返回 `ORPHAN_ARTIFACT_RECONCILIATION_DRY_RUN` 且 `referencing_event_ids=[]` 后，才在同一组精确 ID 后追加 `--apply`。工具仅把 `artifact_status` 从 `active` 改为 `orphaned`，不删除页面，不修改 Event、ExpertState 或 Checkpoint。

## Railway

1. 以 `esm-gateway-v1.1` 为服务根目录。
2. 根据 `.env.example` 配置受保护变量。不需要、也不得添加“写入开关”。
3. 配置单 replica。
4. 将 `PROJECTION_READBACK_WAIT_SECONDS` 设为 `90`；如 Railway 中已存在旧值 `0`，必须手动更新，否则它会覆盖代码默认值。
5. `Procfile` 启动 `python server.py`。
6. Leapility 只配置一个 MCP：`https://<railway-host>/mcp`，Bearer token 使用 `GATEWAY_TOKEN`。

日志全部输出 stdout。正常 Gateway started/completed、Worker started/completed 和 Uvicorn 启动信息为 info；Gateway 工具日志带当前 `instance_id`，确认 token 日志只带长度和 HMAC fingerprint。成功的 `httpx` provider 请求从 INFO 降噪，身份拒绝为 info；可恢复合同/权限问题为 warning；provider 和真实未处理异常为 error。`Terminating session: None` 被过滤。
