#!/usr/bin/env python3
"""Release-only, recoverable cleanup for an otherwise empty real Notion database."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from esm_gateway.errors import ConfigurationError, GatewayError
from esm_gateway.notion import (
    NotionClient,
    _decode_event,
    decode_page,
)


TEST_PREFIX = "[V1-TEST]"
CONFIRM_FLAG = "--confirm-archive-v1-test-data"


def required_environment() -> dict[str, str]:
    names = (
        "NOTION_MAINTENANCE_TOKEN",
        "NOTION_EVENTS_DATA_SOURCE_ID",
        "NOTION_EXPERT_STATE_DATA_SOURCE_ID",
        "NOTION_ARTIFACT_INDEX_DATA_SOURCE_ID",
        "NOTION_PROJECTION_CHECKPOINTS_DATA_SOURCE_ID",
    )
    values = {name: os.environ.get(name, "").strip() for name in names}
    if not all(values.values()):
        raise ConfigurationError("CLEANUP_CONFIGURATION_MISSING")
    return values


async def collect(client: NotionClient, values: dict[str, str]) -> dict[str, Any]:
    event_pages = await client.query(
        values["NOTION_EVENTS_DATA_SOURCE_ID"], page_size=100, fetch_all=True
    )
    state_pages = await client.query(
        values["NOTION_EXPERT_STATE_DATA_SOURCE_ID"], page_size=100, fetch_all=True
    )
    artifact_pages = await client.query(
        values["NOTION_ARTIFACT_INDEX_DATA_SOURCE_ID"], page_size=100, fetch_all=True
    )
    checkpoint_pages = await client.query(
        values["NOTION_PROJECTION_CHECKPOINTS_DATA_SOURCE_ID"],
        page_size=100,
        fetch_all=True,
    )

    states = [(page, decode_page(page)) for page in state_pages]
    artifacts = [(page, decode_page(page)) for page in artifact_pages]
    events = [(page, _decode_event(page)) for page in event_pages]
    checkpoints = [(page, decode_page(page)) for page in checkpoint_pages]

    test_temp_ids = {
        record["temp_application_id"]
        for _page, record in states
        if isinstance(record.get("expert_name"), str)
        and record["expert_name"].startswith(TEST_PREFIX)
    }
    for _page, event in events:
        if event.event_type == "application_created" and str(
            event.payload.get("expert_name", "")
        ).startswith(TEST_PREFIX):
            test_temp_ids.add(event.temp_application_id)

    non_test_states = [
        record
        for _page, record in states
        if record.get("temp_application_id") not in test_temp_ids
    ]
    non_test_artifacts = [
        record
        for _page, record in artifacts
        if record.get("temp_application_id") not in test_temp_ids
    ]
    non_test_events = [
        event for _page, event in events if event.temp_application_id not in test_temp_ids
    ]
    if non_test_states or non_test_artifacts or non_test_events:
        raise ConfigurationError("CLEANUP_ABORTED_NON_TEST_DATA_PRESENT")

    return {
        "events": [page for page, event in events if event.temp_application_id in test_temp_ids],
        "states": [page for page, record in states if record.get("temp_application_id") in test_temp_ids],
        "artifacts": [page for page, record in artifacts if record.get("temp_application_id") in test_temp_ids],
        "checkpoints": [page for page, _record in checkpoints],
        "test_stream_count": len(test_temp_ids),
    }


async def run(confirm: bool) -> int:
    try:
        values = required_environment()
        client = NotionClient(values["NOTION_MAINTENANCE_TOKEN"])
        records = await collect(client, values)
        counts = {
            name: len(records[name])
            for name in ("events", "states", "artifacts", "checkpoints")
        }
        if not confirm:
            print(
                json.dumps(
                    {
                        "status": "preview",
                        "result_code": "V1_TEST_CLEANUP_PREVIEW_READY",
                        "test_prefix": TEST_PREFIX,
                        "test_stream_count": records["test_stream_count"],
                        "record_counts": counts,
                        "writes_performed": False,
                        "required_confirmation_flag": CONFIRM_FLAG,
                    }
                )
            )
            return 0

        # The Railway service must be stopped. These are recoverable trash moves,
        # never permanent deletion. State/material goes first; immutable Events last.
        for category in ("states", "artifacts", "events", "checkpoints"):
            for page in records[category]:
                page_id = page.get("id")
                if not isinstance(page_id, str):
                    raise ConfigurationError("CLEANUP_PROVIDER_RECORD_INVALID")
                await client.archive_page(page_id)
        print(
            json.dumps(
                {
                    "status": "success",
                    "result_code": "V1_TEST_DATA_ARCHIVED",
                    "record_counts": counts,
                    "recoverable_from_notion_trash": True,
                }
            )
        )
        return 0
    except GatewayError as exc:
        print(json.dumps({"status": "failed", "result_code": exc.result_code}))
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(CONFIRM_FLAG, action="store_true")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args.confirm_archive_v1_test_data)))
