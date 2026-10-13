"""Read-only Markdown receipts built from authoritative, complete row manifests."""
from __future__ import annotations

import html
from collections import Counter
from datetime import datetime, timedelta, timezone

SHANGHAI_TZ = timezone(timedelta(hours=8), "Asia/Shanghai")

LABELS = {
    "success": ("完整成功", "资料、已提供资产及专家状态均已核实", "可交接给 Owner"),
    "committed_pending": ("已提交，待同步", "回读或状态投影尚未完成", "查询最新结果，无需重传"),
    "result_unverified": ("结果待核实", "是否提交尚未完全核实", "仅核实原操作，不重复导入"),
    "maintenance_required": ("需维护者处理", "存在部分提交或内容不一致", "交维护者核对"),
    "failed_no_commit": ("未提交失败", "已确定本行没有业务数据提交", "可请求重试预览，仍须精确确认"),
    "not_started": ("尚未开始", "本行尚未执行", "批次结束后核实原上下文，再请求继续预览"),
    "validation_error": ("校验未通过，未导入", "字段或 Owner 校验未通过", "修正文件后重新预检"),
    "duplicate_blocked": ("重复排除，未导入", "命中重复检查", "核实后修正或排除，不覆盖已有专家"),
    "not_selected": ("本次未选择", "未纳入 Admin 确认范围", "按需另行导入"),
}


def cell(value) -> str:
    """Render untrusted values as text, never Markdown structure or active HTML."""
    value = "—" if value is None or value == "" else str(value)
    value = " / ".join(value.splitlines())
    value = "".join(c for c in value if ord(c) >= 32 and not 0x7f <= ord(c) <= 0x9f)
    return "".join(f"&#{ord(c)};" if c in "\\|`*_[]()!#~" else html.escape(c, quote=True) for c in value)


def describe(row):
    label, reason, action = LABELS.get(row["result_status"], LABELS["result_unverified"])
    details = []
    for item in row.get("reasons", []):
        if isinstance(item, dict) and item.get("message"):
            column = item.get("column_letter") or item.get("column")
            details.append((f"{column}：" if column else "") + item["message"])
    if details:
        reason = "；".join(details)
    return label, reason, action


def build_report(batch_id, rows, *, selected_count, running, revision):
    rows = sorted(rows, key=lambda r: r["source_row"])
    counts = dict(Counter(r["result_status"] for r in rows))
    provisional = running or any(counts.get(s) for s in ("committed_pending", "result_unverified", "not_started"))
    title = "专家批量导入" + ("阶段结果报告" if provisional else "结果报告")
    generated_at = datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds")
    lines = [f"# {title}", "", f"批次编号：{cell(batch_id)}", f"生成时间：{generated_at}（Asia/Shanghai）",
             f"执行版本：{revision}", f"原文件专家数：{len(rows)}；本批确认人数：{selected_count}",
             "执行状态：" + ("仍在运行" if running else "本次执行已结束"), "",
             "；".join(f"{LABELS.get(s, LABELS['result_unverified'])[0]}：{n}" for s, n in counts.items()), "",
             "此文档是生成时的结果快照。可预览并手动保存为平台云文档；请求更新结果后会生成新版报告。",
             "结果报告不授权重试；保存文档失败不影响已提交的数据，不要重新导入。", "",
             "| 原行号 | 专家姓名 | Owner ID／姓名 | 导入结果 | 原因／待完成事项 | 下一步 |",
             "| --- | --- | --- | --- | --- | --- |"]
    for row in rows:
        label, reason, action = describe(row)
        owner = f"{row['esm_owner_id']}／{row.get('esm_owner_display_name') or '未匹配到有效 Owner'}"
        lines.append("| " + " | ".join(cell(v) for v in (row["source_row"], row["expert_name"], owner, label, reason, action)) + " |")
    lines += ["", "## 逐人核验明细", ""]
    for row in rows:
        lines += [f"### 第 {row['source_row']} 行 — {cell(row['expert_name'])}", "",
                  "专家ID：" + (cell(row.get("temp_application_id")) if row["result_status"] == "success" else "尚无可交接的已核实专家ID"),
                  f"结果码：{cell(row['result_code'])}"]
        for key, label in (("event_readback_verified", "创建记录"), ("artifact_readback_verified", "资产"), ("projection_readback_verified", "专家状态")):
            value = "已核实" if row.get(key) else "尚未核实"
            if key == "artifact_readback_verified" and not row.get("has_assets", False):
                value = "不适用（未提供资产）"
            if row["result_status"] in {"not_selected", "validation_error", "duplicate_blocked", "not_started", "failed_no_commit"}:
                value = "未执行" if key != "artifact_readback_verified" or row.get("has_assets") else "不适用（未提供资产）"
            lines.append(f"{label}：{value}")
        lines.append("")
    return {"title": title, "mime_type": "text/markdown", "generated_at": generated_at,
            "coverage": "complete", "total_rows": len(rows), "counts": counts,
            "provisional": provisional,
            "markdown": "\n".join(line + "  " if line and not line.startswith(("#", "|")) else line for line in lines)}
