#!/usr/bin/env python3
"""Read-only validation of the frozen four-table Notion schema."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from esm_gateway.errors import GatewayError
from esm_gateway.notion import storage_from_environment


async def run() -> int:
    try:
        result = await storage_from_environment().preflight()
    except GatewayError as exc:
        print(json.dumps({"status": "failed", "result_code": exc.result_code}))
        return 1
    print(
        json.dumps(
            {
                "status": "success",
                "result_code": "NOTION_SCHEMA_PREFLIGHT_PASSED",
                "tables": result,
                "writes_performed": False,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
