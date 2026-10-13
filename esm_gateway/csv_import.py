"""Deterministic CSV parsing; never execute or repair uploaded cell contents."""
from __future__ import annotations

import csv
import io
import re
from urllib.parse import urlsplit, urlunsplit

from .contracts import MARKET_REGION_VALUES, PROFESSIONAL_DOMAIN_VALUES
from .errors import ContractError

HEADERS = (
    "expert_name", "source", "esm_owner_id", "primary_contact_email",
    "acquisition_campaign", "professional_domains", "market_regions",
    "primary_public_profile_url", "admin_import_note_short", "expert_agent_name",
    "expert_agent_url", "leapility_card_url",
)
MARKER = "#填写说明："
MAX_BYTES = 1024 * 1024
LIMITS = {"expert_name": 100, "primary_contact_email": 254,
          "acquisition_campaign": 200, "primary_public_profile_url": 2000,
          "admin_import_note_short": 280, "expert_agent_name": 200,
          "expert_agent_url": 2000, "leapility_card_url": 2000}
URL_FIELDS = ("primary_public_profile_url", "expert_agent_url", "leapility_card_url")


def normalize_url(value: str) -> str:
    """Normalize only scheme and hostname; preserve path, query and fragment."""
    parts = urlsplit(value)
    host = parts.hostname or ""
    if ":" in host:
        host = "[" + host + "]"
    port = ":" + str(parts.port) if parts.port is not None else ""
    return urlunsplit((parts.scheme.lower(), host.lower() + port,
                       parts.path, parts.query, parts.fragment))


def valid_url(value: str) -> bool:
    try:
        parts = urlsplit(value)
        host = parts.hostname
        _ = parts.port
        return bool(parts.scheme.lower() == "https" and host
                    and not parts.username and not parts.password
                    and not re.search(r"[\s\x00-\x1f\x7f]", value)
                    and not any(c in host for c in "\\/%?#"))
    except ValueError:
        return False


def parse_csv(text: str) -> list[dict]:
    if not isinstance(text, str):
        raise ContractError("CSV_TEXT_REQUIRED")
    try:
        size = len(text.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ContractError("CSV_ENCODING_INVALID") from exc
    if size > MAX_BYTES:
        raise ContractError("CSV_FILE_TOO_LARGE", "CSV 文件最大 1 MiB。")
    try:
        csv.field_size_limit(MAX_BYTES)
        records = list(csv.reader(io.StringIO(text.removeprefix("\ufeff"), newline=""), strict=True))
    except csv.Error as exc:
        raise ContractError("CSV_STRUCTURE_INVALID", "CSV 引号或字段结构损坏。") from exc
    if not records or records[0] != list(HEADERS):
        raise ContractError("CSV_HEADER_INVALID", "请保留模板的 12 列表头及顺序。")
    if len(records) < 2 or len(records[1]) != 12 or not records[1][0].strip().startswith(MARKER):
        raise ContractError("CSV_INSTRUCTION_ROW_INVALID", "第 2 行 A 列缺少填写说明标识，请保留模板前两行。")
    rows = []
    for number, cells in enumerate(records[2:], 3):
        if not any(cell.strip() for cell in cells):
            continue
        if len(cells) != 12:
            raise ContractError("CSV_COLUMN_COUNT_INVALID", f"第 {number} 行须有 12 列。")
        if cells[0].strip().startswith(MARKER):
            raise ContractError("CSV_INSTRUCTION_ROW_DUPLICATED", f"第 {number} 行填写说明重复或错位。")
        rows.append({"source_row": number, "values": dict(zip(HEADERS, (c.strip() for c in cells)))})
    if not 1 <= len(rows) <= 30:
        raise ContractError("CSV_ROW_COUNT_INVALID", "请填写 1–30 位专家，表头和说明行不计人数。")
    return rows


def validate_row(row: dict, roster: dict) -> dict:
    values = row["values"].copy()
    errors = []

    def error(field: str, code: str, message: str, example: str = "") -> None:
        errors.append({"source_row": row["source_row"], "column": field,
                       "column_letter": chr(65 + HEADERS.index(field)),
                       "error_code": code, "message": message, "example": example})

    if not values["expert_name"]:
        error("expert_name", "REQUIRED", "请填写专家完整姓名。", "张三")
    if values["source"] not in {"ordinary_inbound", "important_inbound"}:
        error("source", "ENUM_INVALID", "请填写 ordinary_inbound 或 important_inbound。", "ordinary_inbound")
    owner = roster.get(values["esm_owner_id"])
    if owner is None:
        error("esm_owner_id", "OWNER_INVALID", "请从当前 ESM 名单复制有效 Owner ID。")
    for field, limit in LIMITS.items():
        if len(values[field]) > limit:
            error(field, "TOO_LONG", f"最多 {limit} 字符，请缩短后重传。")
    email = values["primary_contact_email"]
    if email and not re.fullmatch(r"[^\s@,;]+@[^\s@,;]+\.[^\s@,;]+", email):
        error("primary_contact_email", "EMAIL_INVALID", "请填写一个合法邮箱或留空。", "zhangsan@example.com")
    values["primary_contact_email"] = email.lower()
    for field in URL_FIELDS:
        if values[field] and not valid_url(values[field]):
            error(field, "URL_INVALID", "请填写单个合法 HTTPS 链接，不含账号密码或未编码空白。", "https://example.com/zhangsan")
    if values["expert_agent_name"] and not values["expert_agent_url"]:
        error("expert_agent_name", "URL_REQUIRED", "数字分身名称需要 K 列链接；请补链接或清空名称。")
    for field, allowed in (("professional_domains", PROFESSIONAL_DOMAIN_VALUES),
                           ("market_regions", MARKET_REGION_VALUES - {"emea"})):
        raw = values[field]
        items = list(dict.fromkeys(part.strip() for part in raw.split(";"))) if raw else []
        if not set(items).issubset(allowed):
            error(field, "ENUM_INVALID", "请使用模板中的英文枚举，多值用英文分号分隔。", ";".join(sorted(allowed)))
        values[field] = items
    return {"source_row": row["source_row"], "values": values, "errors": errors,
            "owner_display_name": owner.display_name if owner else None,
            "asset_titles": {
                "expert_agent_link": (values["expert_agent_name"] or values["expert_name"] + "的数字分身") if values["expert_agent_url"] else None,
                "leapility_card_link": values["expert_name"] + "的 Leapility Card" if values["leapility_card_url"] else None,
            }, "duplicates": [], "warnings": []}
