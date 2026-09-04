#!/usr/bin/env python3
"""Safely mark one exact, unreferenced active Artifact as orphaned.

The command is read-only unless ``--apply`` is supplied. It never deletes a
Notion page and never changes Events, ExpertState, or Checkpoint.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from esm_gateway.errors import ContractError, GatewayError, PersistenceError
from esm_gateway.notion import storage_from_environment
from esm_gateway.storage import Storage


async def reconcile(
    storage: Storage,
    *,
    artifact_id: str,
    temp_application_id: str,
    apply: bool,
) -> dict[str, Any]:
    artifact = await storage.get_artifact(artifact_id)
    if artifact is None or artifact.temp_application_id != temp_application_id:
        raise ContractError("ARTIFACT_NOT_FOUND")
    if artifact.artifact_status != "active":
        raise ContractError("ARTIFACT_NOT_ACTIVE")

    events = await storage.stream_events(temp_application_id)
    referencing_event_ids = [
        event.event_id
        for event in events
        if artifact_id in event.artifact_reference_ids
    ]
    if referencing_event_ids:
        raise ContractError("ARTIFACT_STILL_REFERENCED")

    base = {
        "artifact_id": artifact_id,
        "temp_application_id": temp_application_id,
        "previous_artifact_status": artifact.artifact_status,
        "target_artifact_status": "orphaned",
        "referencing_event_ids": [],
        "events_changed": False,
        "expert_state_changed": False,
        "checkpoint_changed": False,
        "page_deleted": False,
    }
    if not apply:
        return {
            "status": "success",
            "result_code": "ORPHAN_ARTIFACT_RECONCILIATION_DRY_RUN",
            "writes_performed": False,
            "would_change": True,
            **base,
        }

    updated = await storage.mark_artifact_orphaned(
        artifact_id, temp_application_id
    )
    if updated.artifact_status != "orphaned":
        raise ContractError("ARTIFACT_ORPHAN_RECONCILIATION_FAILED")
    return {
        "status": "success",
        "result_code": "ORPHAN_ARTIFACT_MARKED",
        "writes_performed": True,
        "would_change": False,
        **base,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run or mark one exact active Artifact as orphaned after proving "
            "that no Event for the same temp_application_id references it."
        )
    )
    parser.add_argument("--artifact-id", required=True)
    parser.add_argument("--temp-application-id", required=True)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Perform the status update. Without this flag the command is read-only.",
    )
    return parser.parse_args()


async def run() -> int:
    args = parse_args()
    try:
        result = await reconcile(
            storage_from_environment(),
            artifact_id=args.artifact_id.strip(),
            temp_application_id=args.temp_application_id.strip(),
            apply=args.apply,
        )
    except GatewayError as exc:
        write_may_have_succeeded = (
            exc.write_may_have_succeeded
            if isinstance(exc, PersistenceError)
            else False
        )
        print(
            json.dumps(
                {
                    "status": "failed",
                    "result_code": exc.result_code,
                    "failed_phase": (
                        exc.failed_phase
                        if isinstance(exc, PersistenceError)
                        else None
                    ),
                    "write_may_have_succeeded": write_may_have_succeeded,
                    "writes_performed": (
                        None if write_may_have_succeeded else False
                    ),
                },
                ensure_ascii=False,
            )
        )
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
