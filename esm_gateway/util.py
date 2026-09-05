"""Deterministic serialization, hashing, IDs, and time helpers."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_text(value: datetime | None = None) -> str:
    instant = value or utc_now()
    return instant.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def utc_minute_text(value: datetime | None = None) -> str:
    """Return the UTC minute precision preserved by Notion date properties."""

    instant = (value or utc_now()).astimezone(timezone.utc).replace(
        second=0, microsecond=0
    )
    return instant.isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_value(value: Any) -> str:
    return sha256_text(canonical_json(value))


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def new_temp_application_id() -> str:
    return f"TMP-{str(uuid.uuid4()).upper()}"
