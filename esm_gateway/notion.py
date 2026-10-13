"""Strict Notion adapter for the frozen four-database V1 schema.

Provider responses, database identifiers, and tokens never leave this module.
The adapter validates schema read-only and never creates or mutates properties.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from copy import deepcopy
from typing import Any

import httpx

from .contracts import Artifact, Event, ExpertState
from .errors import ConfigurationError, ContractError, PersistenceError
from .storage import (
    ArtifactWriteReceipt,
    EventDraft,
    artifact_mismatch_fields,
    artifacts_equivalent,
)
from .util import canonical_json, new_id, sha256_value, utc_text


NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = os.environ.get("NOTION_VERSION", "2026-03-11")
NOTION_TIMEOUT_SECONDS = 15.0
NOTION_READ_RETRY_ATTEMPTS = 3
NOTION_READ_RETRY_BASE_DELAY_SECONDS = 0.5
PROJECTION_VERSION = "1.2.0"
CHECKPOINT_ID = "esm-v1-projector:all"


logger = logging.getLogger("esm-gateway.notion")


EVENT_FIELDS = {
    "event_id": "title",
    "global_sequence": "number",
    "event_type": "select",
    "stream_version": "number",
    "application_id": "rich_text",
    "temp_application_id": "rich_text",
    "occurred_at": "date",
    "recorded_at": "date",
    "actor_type": "select",
    "actor_id": "rich_text",
    "esm_owner_id_at_event_time": "rich_text",
    "esm_owner_display_name_at_event_time": "rich_text",
    "esm_owner_user_at_event_time": "people",
    "source_session_id": "rich_text",
    "execution_id": "rich_text",
    "correlation_id": "rich_text",
    "causation_id": "rich_text",
    "idempotency_key": "rich_text",
    "command_hash": "rich_text",
    "schema_version": "rich_text",
    "payload": "rich_text",
    "payload_hash": "rich_text",
    "artifact_reference_ids": "rich_text",
    "target_event_id": "rich_text",
    "ingested_at": "created_time",
    "ingested_by": "created_by",
}

STATE_FIELDS = {
    "expert_name": "title",
    "application_id": "rich_text",
    "temp_application_id": "rich_text",
    "source": "select",
    "sourcing_run_id": "rich_text",
    "esm_owner_id": "rich_text",
    "esm_owner_display_name": "rich_text",
    "esm_owner_user": "people",
    "professional_domains": "multi_select",
    "market_regions": "multi_select",
    "primary_public_profile_url": "url",
    "identity_confidence": "select",
    "sourcing_priority": "select",
    "candidate_readiness": "select",
    "current_stage": "select",
    "stage_status": "select",
    "stage_outcome": "select",
    "suspended_from_status": "select",
    "failure_count": "number",
    "retry_after": "date",
    "loop_count": "number",
    "next_action": "rich_text",
    "next_action_due_at": "date",
    "requires_esm_confirmation": "checkbox",
    "close_reason_code": "select",
    "close_reason_summary": "rich_text",
    "dropped_stage": "select",
    "requires_reactivation": "checkbox",
    "last_meaningful_action_at": "date",
    "sourcing_decision": "select",
    "sourcing_decided_at": "date",
    "screening_result": "select",
    "screening_decided_at": "date",
    "evaluation_summary_short": "rich_text",
    "outreach_channel": "select",
    "outreach_status": "select",
    "outreach_sent_at": "date",
    "outreach_response_received_at": "date",
    "outreach_response_category": "select",
    "outreach_response_summary_short": "rich_text",
    "discovery_scheduled_at": "date",
    "discovery_completed_at": "date",
    "discovery_source_registered": "checkbox",
    "discovery_outcome": "select",
    "discovery_decided_at": "date",
    "self_onboarding_capable": "select",
    "pipeline_track": "select",
    "positioning_scheduled_at": "date",
    "positioning_completed_at": "date",
    "positioning_meeting_count": "number",
    "positioning_source_registered": "checkbox",
    "positioning_outcome": "select",
    "positioning_decided_at": "date",
    "first_kit_direction_short": "rich_text",
    "onboarding_started_at": "date",
    "onboarding_handoff_completed_at": "date",
    "credits_tutorial_sent_at": "date",
    "mvp_agent_kickoff_at": "date",
    "positioning_result_discussion_at": "date",
    "official_launch_at": "date",
    "sourcing_final_artifact_id": "rich_text",
    "evaluation_final_artifact_id": "rich_text",
    "outreach_final_artifact_id": "rich_text",
    "discovery_prep_final_artifact_id": "rich_text",
    "discovery_source_artifact_ids": "rich_text",
    "discovery_recap_final_artifact_id": "rich_text",
    "expert_conversation_artifact_id": "rich_text",
    "expert_conversation_access_status": "select",
    "expert_agent_artifact_id": "rich_text",
    "expert_agent_access_status": "select",
    "positioning_prep_final_artifact_id": "rich_text",
    "positioning_source_artifact_ids": "rich_text",
    "positioning_recap_final_artifact_id": "rich_text",
    "last_applied_event_id": "rich_text",
    "last_processed_stream_version": "number",
    "last_processed_global_sequence": "number",
    "projection_version": "rich_text",
    "projection_updated_at": "date",
    "projection_conflict": "checkbox",
    "projection_conflict_code": "select",
    "state_hash": "rich_text",
    "created_at": "date",
    "updated_at": "date",
    "last_actor_id": "rich_text",
}

ARTIFACT_FIELDS = {
    "artifact_id": "title",
    "logical_artifact_id": "rich_text",
    "artifact_version": "number",
    "artifact_schema_version": "rich_text",
    "artifact_type": "select",
    "artifact_kind": "select",
    "version_role": "select",
    "artifact_status": "select",
    "source_execution_id": "rich_text",
    "source_result_hash": "rich_text",
    "application_id": "rich_text",
    "temp_application_id": "rich_text",
    "expert_name_snapshot": "rich_text",
    "esm_owner_id": "rich_text",
    "esm_owner_display_name": "rich_text",
    "esm_owner_user": "people",
    "stage": "select",
    "display_title": "rich_text",
    "storage_type": "select",
    "content_page_or_object_id": "rich_text",
    "controlled_access_url": "url",
    "mime_type": "rich_text",
    "size_bytes": "number",
    "language": "multi_select",
    "content_hash": "rich_text",
    "created_at": "date",
    "created_by_actor_type": "select",
    "created_by_actor_id": "rich_text",
    "reviewed_at": "date",
    "reviewed_by_actor_id": "rich_text",
    "supersedes_artifact_id": "rich_text",
    "source_artifact_ids": "rich_text",
    "access_policy": "select",
    "allowed_purposes": "multi_select",
    "link_access_status": "select",
    "link_last_checked_at": "date",
    "readable_content_descriptor": "rich_text",
    "ingestion_status": "select",
    "security_scan_status": "select",
    "retention_until": "date",
}

CHECKPOINT_FIELDS = {
    "checkpoint_id": "title",
    "projector_name": "rich_text",
    "projector_version": "rich_text",
    "supported_event_schema_versions": "multi_select",
    "shard_key": "rich_text",
    "status": "select",
    "run_mode": "select",
    "last_processed_global_sequence": "number",
    "last_processed_event_id": "rich_text",
    "last_successful_checkpoint_at": "date",
    "current_run_started_at": "date",
    "current_run_completed_at": "date",
    "replay_from_global_sequence": "number",
    "replay_temp_application_id": "rich_text",
    "lease_owner": "rich_text",
    "lease_expires_at": "date",
    "events_scanned_last_run": "number",
    "streams_succeeded_last_run": "number",
    "streams_failed_last_run": "number",
    "dead_letter_count": "number",
    "projection_lag_seconds": "number",
    "last_error_code": "select",
    "last_error_summary": "rich_text",
    "updated_at": "date",
}


# These populated legacy Artifact Index properties must remain in Notion. They
# are outside the V1 Gateway contract: schema preflight permits them, while all
# Gateway encode/decode paths continue to ignore them so existing values are
# never read, written, or cleared by this service.
PRESERVED_EXTRA_FIELDS: dict[str, frozenset[str]] = {
    "artifact": frozenset(
        {
            "confirmed_by_actor_id",
            "review_confirmation_event_id",
            "source_stage",
        }
    ),
}


SELECT_OPTIONS: dict[tuple[str, str], set[str]] = {
    ("events", "event_type"): {
        "application_created", "expert_profile_updated", "esm_owner_transferred",
        "sourcing_assessment_recorded", "stage_processing_started",
        "stage_ai_draft_generated", "stage_review_requested",
        "stage_reviewed_final_confirmed", "call_prep_review_completed",
        "stage_paused", "stage_resumed", "stage_processing_failed",
        "stage_retry_requested", "intervention_requested", "intervention_resolved",
        "application_canceled", "application_reopened", "next_action_set",
        "reactivation_requested", "sourcing_outcome_confirmed",
        "evaluation_outcome_confirmed", "outreach_sent",
        "outreach_response_recorded", "outreach_outcome_confirmed",
        "discovery_call_scheduled", "discovery_call_completed",
        "discovery_source_registered", "discovery_outcome_confirmed",
        "positioning_call_scheduled", "positioning_call_completed",
        "positioning_source_registered", "positioning_outcome_confirmed",
        "onboarding_handoff_started", "onboarding_handoff_completed",
        "credits_tutorial_sent", "mvp_agent_kickoff_scheduled",
        "positioning_result_discussion_scheduled", "expert_officially_launched",
        "event_corrected", "event_voided",
    },
    ("events", "actor_type"): {"esm", "agent", "system", "admin", "integration"},
    ("state", "source"): {"inbound", "esm_outbound", "ordinary_inbound", "important_inbound"},
    ("state", "professional_domains"): {
        "marketing", "sales", "gtm", "international_trade", "cross_border_ecommerce",
        "product", "operations", "people_hr", "finance",
        "legal", "healthcare", "technology", "education", "creator_economy",
        "professional_services", "financial_services", "retail_ecommerce",
        "manufacturing_industrial", "real_estate", "construction_infrastructure",
        "transportation_logistics", "hospitality_travel", "media_entertainment",
        "energy_natural_resources", "agriculture_food", "life_sciences",
        "government_public_policy", "nonprofit_social_impact", "other",
    },
    ("state", "market_regions"): {
        "na", "latam", "europe", "middle_east", "africa", "apac",
        "china_mainland", "greater_china", "global", "other", "emea",
    },
    ("state", "current_stage"): {
        "sourcing", "evaluation", "initial_outreach", "discovery_call",
        "agent_positioning", "onboarding_handoff",
    },
    ("state", "stage_status"): {
        "ready", "in_progress", "ai_draft_ready", "awaiting_esm_review",
        "reviewed_final_ready", "prep_ready", "prep_in_progress",
        "prep_ai_draft_ready", "awaiting_prep_review", "ready_to_send",
        "awaiting_reply", "reply_received", "awaiting_schedule", "awaiting_progress_assessment", "ready_for_call",
        "call_completed_awaiting_source", "recap_ready", "recap_in_progress",
        "recap_ai_draft_ready", "awaiting_recap_review", "paused",
        "failed_retryable", "intervention_required", "canceled", "closed", "completed",
    },
    ("state", "suspended_from_status"): {
        "ready", "in_progress", "ai_draft_ready", "awaiting_esm_review",
        "reviewed_final_ready", "prep_ready", "prep_in_progress",
        "prep_ai_draft_ready", "awaiting_prep_review", "ready_to_send",
        "awaiting_reply", "reply_received", "awaiting_schedule", "awaiting_progress_assessment", "ready_for_call",
        "call_completed_awaiting_source", "recap_ready", "recap_in_progress",
        "recap_ai_draft_ready", "awaiting_recap_review", "paused",
        "failed_retryable", "intervention_required", "canceled", "closed", "completed",
    },
    ("state", "stage_outcome"): {
        "enter_evaluation", "do_not_enter", "advance", "pause", "reject",
        "discovery_scheduled", "proceed", "re_evaluate", "advance_to_onboarding",
        "rework", "handed_off",
    },
    ("state", "identity_confidence"): {"high", "medium", "low", "unknown"},
    ("state", "sourcing_priority"): {"p0", "p1", "hold"},
    ("state", "candidate_readiness"): {"ready", "needs_disambiguation", "low_public_surface"},
    ("state", "self_onboarding_capable"): {"yes", "no", "unsure"},
    ("state", "pipeline_track"): {"expert_led", "esm_led", "hybrid"},
    ("state", "outreach_status"): {
        "not_started", "prepared", "sent", "awaiting_reply", "replied",
        "discovery_scheduled", "paused", "rejected",
    },
    ("state", "sourcing_decision"): {"enter_evaluation", "do_not_enter"},
    ("state", "screening_result"): {"advance", "pause", "reject"},
    ("state", "outreach_channel"): {"email", "linkedin", "x", "whatsapp", "wechat", "phone", "other"},
    ("state", "outreach_response_category"): {"positive", "neutral", "negative", "no_decision"},
    ("state", "discovery_outcome"): {"proceed", "re_evaluate", "reject"},
    ("state", "positioning_outcome"): {"advance_to_onboarding", "rework", "pause", "reject"},
    ("state", "close_reason_code"): {
        "insufficient_evidence", "low_fit", "no_response", "expert_declined",
        "timing", "capacity", "compliance", "duplicate", "canceled_by_esm", "other",
    },
    ("state", "dropped_stage"): {
        "sourcing", "evaluation", "initial_outreach", "discovery_call",
        "agent_positioning", "onboarding_handoff",
    },
    ("state", "expert_conversation_access_status"): {
        "not_provided", "not_checked", "accessible", "login_required", "inaccessible",
    },
    ("state", "expert_agent_access_status"): {
        "not_provided", "not_checked", "accessible", "login_required", "inaccessible",
    },
    ("state", "projection_conflict_code"): {
        "unknown_event", "invalid_transition", "missing_predecessor", "schema_unsupported",
        "artifact_invalid", "state_hash_mismatch", "other",
    },
    ("artifact", "artifact_type"): {
        "public_profile_reference", "sourcing_report", "evaluation_report",
        "initial_outreach_material", "outreach_response", "discovery_call_prep",
        "discovery_call_transcript", "discovery_call_notes", "discovery_call_recap",
        "expert_conversation_link", "expert_agent_link", "leapility_card_link", "positioning_call_prep",
        "positioning_call_transcript", "positioning_call_notes",
        "positioning_call_recap", "esm_work_note",
    },
    ("artifact", "artifact_kind"): {"work_product", "reference_source", "external_reference", "operational"},
    ("artifact", "storage_type"): {"notion_page", "managed_file", "controlled_external_url"},
    ("artifact", "version_role"): {"ai_draft", "human_supplement", "reviewed_final", "source"},
    ("artifact", "artifact_status"): {"active", "superseded", "voided", "orphaned", "quarantined"},
    ("artifact", "stage"): {
        "sourcing", "evaluation", "initial_outreach", "discovery_call",
        "agent_positioning", "onboarding_handoff",
    },
    ("artifact", "created_by_actor_type"): {"esm", "agent", "system", "admin", "integration"},
    ("artifact", "access_policy"): {"owner_only", "task_scoped"},
    ("artifact", "allowed_purposes"): {
        "sourcing", "evaluation", "outreach", "discovery_prep", "discovery_recap",
        "positioning_prep", "positioning_recap", "onboarding_handoff",
    },
    ("artifact", "link_access_status"): {"not_checked", "accessible", "login_required", "inaccessible"},
    ("artifact", "ingestion_status"): {"pending", "ready", "failed", "quarantined"},
    ("artifact", "security_scan_status"): {"not_scanned", "passed", "flagged", "not_applicable"},
    ("checkpoint", "supported_event_schema_versions"): {"1.0.0", "1.1.0", "1.2.0"},
    ("checkpoint", "status"): {"idle", "running", "failed", "paused", "rebuilding"},
    ("checkpoint", "run_mode"): {"incremental", "full_replay", "targeted_replay"},
    ("checkpoint", "last_error_code"): {
        "event_read_failed", "state_write_failed", "contract_invalid",
        "projection_conflict", "lease_lost", "checkpoint_invalid",
        "artifact_lookup_failed", "unknown",
    },
}


def _plain_text(items: Any) -> str | None:
    if not isinstance(items, list):
        return None
    parts: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            raise PersistenceError("NOTION_RESPONSE_INVALID")
        plain = item.get("plain_text")
        if isinstance(plain, str):
            parts.append(plain)
        elif isinstance(item.get("text"), dict) and isinstance(item["text"].get("content"), str):
            parts.append(item["text"]["content"])
        else:
            raise PersistenceError("NOTION_RESPONSE_INVALID")
    return "".join(parts)


def decode_property(prop: Any) -> Any:
    if not isinstance(prop, dict):
        return None
    prop_type = prop.get("type")
    if prop_type in {"title", "rich_text"}:
        return _plain_text(prop.get(prop_type))
    if prop_type == "number":
        return prop.get("number")
    if prop_type in {"select", "status"}:
        value = prop.get(prop_type)
        return value.get("name") if isinstance(value, dict) else None
    if prop_type == "multi_select":
        values = prop.get("multi_select")
        return [item["name"] for item in values if isinstance(item, dict) and isinstance(item.get("name"), str)] if isinstance(values, list) else []
    if prop_type == "checkbox":
        return bool(prop.get("checkbox"))
    if prop_type == "date":
        value = prop.get("date")
        return value.get("start") if isinstance(value, dict) else None
    if prop_type in {"url", "created_time", "last_edited_time"}:
        return prop.get(prop_type)
    if prop_type == "people":
        values = prop.get("people")
        return [item["id"] for item in values if isinstance(item, dict) and isinstance(item.get("id"), str)] if isinstance(values, list) else []
    return None


def decode_page(page: Any) -> dict[str, Any]:
    if not isinstance(page, dict) or not isinstance(page.get("id"), str):
        raise PersistenceError("NOTION_RESPONSE_INVALID")
    properties = page.get("properties")
    if not isinstance(properties, dict):
        raise PersistenceError("NOTION_RESPONSE_INVALID")
    result = {name: decode_property(value) for name, value in properties.items()}
    result["_page_id"] = page["id"]
    return result


def encode_property(prop_type: str, value: Any) -> dict[str, Any]:
    if prop_type in {"title", "rich_text"}:
        if value is None or value == "":
            return {prop_type: []}
        content = value if isinstance(value, str) else canonical_json(value)
        return {prop_type: [{"type": "text", "text": {"content": content[i:i + 2000]}} for i in range(0, len(content), 2000)]}
    if prop_type == "number":
        return {"number": value}
    if prop_type in {"select", "status"}:
        return {prop_type: None if value is None or value == "" else {"name": str(value)}}
    if prop_type == "multi_select":
        return {"multi_select": [{"name": str(item)} for item in (value or [])]}
    if prop_type == "checkbox":
        return {"checkbox": bool(value)}
    if prop_type == "date":
        return {"date": None if value is None or value == "" else {"start": str(value)}}
    if prop_type == "url":
        return {"url": value or None}
    if prop_type == "people":
        return {"people": [{"id": str(item)} for item in (value or [])]}
    raise PersistenceError("NOTION_PROPERTY_TYPE_UNSUPPORTED")


def encode_values(fields: dict[str, str], values: dict[str, Any]) -> dict[str, Any]:
    encoded: dict[str, Any] = {}
    for name, prop_type in fields.items():
        if prop_type in {"created_time", "created_by"} or name not in values:
            continue
        encoded[name] = encode_property(prop_type, values[name])
    return encoded


def equals_filter(fields: dict[str, str], name: str, value: Any) -> dict[str, Any]:
    prop_type = fields.get(name)
    if prop_type in {"title", "rich_text", "url", "select"}:
        return {"property": name, prop_type: {"equals": str(value)}}
    if prop_type == "number":
        return {"property": name, "number": {"equals": value}}
    raise PersistenceError("NOTION_FILTER_UNSUPPORTED")


class NotionClient:
    def __init__(self, token: str) -> None:
        if not token:
            raise ConfigurationError("NOTION_CONFIGURATION_MISSING")
        self._token = token

    async def request(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        write: bool = False,
    ) -> dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Notion-Version": NOTION_VERSION,
        }
        attempts = 1 if write else NOTION_READ_RETRY_ATTEMPTS
        for attempt in range(1, attempts + 1):
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(NOTION_TIMEOUT_SECONDS, connect=5.0)
                ) as client:
                    response = await client.request(
                        method,
                        f"{NOTION_API_BASE}{path}",
                        headers=headers,
                        json=body,
                    )
                break
            except Exception as exc:
                if attempt >= attempts:
                    raise PersistenceError(
                        "NOTION_REQUEST_FAILED", write_may_have_succeeded=write
                    ) from exc
                logger.warning(
                    "notion_read_request_retry attempt=%s max_attempts=%s",
                    attempt,
                    attempts,
                )
                await asyncio.sleep(
                    NOTION_READ_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
                )
        if response.status_code < 200 or response.status_code >= 300:
            raise PersistenceError(
                "NOTION_REQUEST_REJECTED", write_may_have_succeeded=write
            )
        try:
            result = response.json()
        except Exception as exc:
            raise PersistenceError(
                "NOTION_RESPONSE_INVALID", write_may_have_succeeded=write
            ) from exc
        if not isinstance(result, dict):
            raise PersistenceError("NOTION_RESPONSE_INVALID", write_may_have_succeeded=write)
        return result

    async def schema(self, data_source_id: str) -> dict[str, Any]:
        return await self.request("GET", f"/data_sources/{data_source_id}")

    async def retrieve_page(self, page_id: str) -> dict[str, Any]:
        return await self.request("GET", f"/pages/{page_id}")

    async def query(
        self,
        data_source_id: str,
        *,
        filter_body: dict[str, Any] | None = None,
        sorts: list[dict[str, Any]] | None = None,
        page_size: int = 100,
        fetch_all: bool = False,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            body: dict[str, Any] = {"page_size": min(page_size, 100)}
            if filter_body is not None:
                body["filter"] = filter_body
            if sorts is not None:
                body["sorts"] = sorts
            if cursor:
                body["start_cursor"] = cursor
            response = await self.request(
                "POST", f"/data_sources/{data_source_id}/query", body=body
            )
            page = response.get("results")
            if not isinstance(page, list):
                raise PersistenceError("NOTION_RESPONSE_INVALID")
            results.extend(item for item in page if isinstance(item, dict))
            if not fetch_all or not response.get("has_more"):
                return results
            cursor = response.get("next_cursor")
            if not isinstance(cursor, str) or not cursor:
                raise PersistenceError("NOTION_RESPONSE_INVALID")

    async def create_page(self, data_source_id: str, properties: dict[str, Any]) -> dict[str, Any]:
        return await self.request(
            "POST",
            "/pages",
            body={
                "parent": {"type": "data_source_id", "data_source_id": data_source_id},
                "properties": properties,
            },
            write=True,
        )

    async def update_page(self, page_id: str, properties: dict[str, Any]) -> dict[str, Any]:
        return await self.request(
            "PATCH", f"/pages/{page_id}", body={"properties": properties}, write=True
        )

    async def archive_page(self, page_id: str) -> None:
        result = await self.request(
            "PATCH", f"/pages/{page_id}", body={"archived": True}, write=True
        )
        if result.get("archived") is not True:
            raise PersistenceError(
                "NOTION_ARCHIVE_READBACK_FAILED", write_may_have_succeeded=True
            )


def validate_schema(
    kind: str,
    schema: dict[str, Any],
    expected_fields: dict[str, str],
) -> None:
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        raise ConfigurationError(
            "NOTION_SCHEMA_INVALID",
            (
                "NOTION_SCHEMA_INVALID "
                f"table={kind} properties_object_missing=true"
            ),
        )
    actual_fields = set(properties)
    required_fields = set(expected_fields)
    preserved_extra_fields = set(PRESERVED_EXTRA_FIELDS.get(kind, frozenset()))
    missing_fields = sorted(required_fields - actual_fields)
    unexpected_fields = sorted(
        actual_fields - required_fields - preserved_extra_fields
    )
    if missing_fields or unexpected_fields:
        raise ConfigurationError(
            "NOTION_SCHEMA_FIELD_MISMATCH",
            (
                "NOTION_SCHEMA_FIELD_MISMATCH "
                f"table={kind} missing_fields={missing_fields!r} "
                f"unexpected_fields={unexpected_fields!r}"
            ),
        )
    for name, expected_type in expected_fields.items():
        spec = properties.get(name)
        if not isinstance(spec, dict) or spec.get("type") != expected_type:
            actual_type = spec.get("type") if isinstance(spec, dict) else None
            raise ConfigurationError(
                "NOTION_SCHEMA_TYPE_MISMATCH",
                (
                    "NOTION_SCHEMA_TYPE_MISMATCH "
                    f"table={kind} field={name!r} "
                    f"expected_type={expected_type!r} actual_type={actual_type!r}"
                ),
            )
        expected_options = SELECT_OPTIONS.get((kind, name))
        if expected_options is None:
            continue
        option_spec = spec.get(expected_type)
        options = option_spec.get("options") if isinstance(option_spec, dict) else None
        actual = {
            item["name"]
            for item in options or []
            if isinstance(item, dict) and isinstance(item.get("name"), str)
        }
        if actual != expected_options:
            missing_options = sorted(expected_options - actual)
            unexpected_options = sorted(actual - expected_options)
            raise ConfigurationError(
                "NOTION_SCHEMA_ENUM_MISMATCH",
                (
                    "NOTION_SCHEMA_ENUM_MISMATCH "
                    f"table={kind} field={name!r} "
                    f"missing_options={missing_options!r} "
                    f"unexpected_options={unexpected_options!r}"
                ),
            )


class NotionStorage:
    """Four-table provider implementation for one Railway process/replica."""

    def __init__(
        self,
        *,
        event_reader_token: str,
        event_writer_token: str,
        state_store_token: str,
        artifact_store_token: str,
        checkpoint_store_token: str,
        esm_agent_reader_token: str,
        events_data_source_id: str,
        state_data_source_id: str,
        artifact_data_source_id: str,
        checkpoint_data_source_id: str,
    ) -> None:
        self.event_reader = NotionClient(event_reader_token)
        self.event_writer = NotionClient(event_writer_token)
        self.state_store = NotionClient(state_store_token)
        self.artifact_store = NotionClient(artifact_store_token)
        self.checkpoint_store = NotionClient(checkpoint_store_token)
        self.agent_reader = NotionClient(esm_agent_reader_token)
        self.events_id = events_data_source_id
        self.state_id = state_data_source_id
        self.artifact_id = artifact_data_source_id
        self.checkpoint_id = checkpoint_data_source_id
        if not all((self.events_id, self.state_id, self.artifact_id, self.checkpoint_id)):
            raise ConfigurationError("NOTION_CONFIGURATION_MISSING")
        self._append_lock = asyncio.Lock()
        self._next_sequence: int | None = None
        self._artifact_commit_acks: dict[str, Artifact] = {}

    async def preflight(self) -> dict[str, str]:
        schemas = await asyncio.gather(
            self.event_reader.schema(self.events_id),
            self.agent_reader.schema(self.state_id),
            self.agent_reader.schema(self.artifact_id),
            self.checkpoint_store.schema(self.checkpoint_id),
        )
        for kind, schema, fields in zip(
            ("events", "state", "artifact", "checkpoint"),
            schemas,
            (EVENT_FIELDS, STATE_FIELDS, ARTIFACT_FIELDS, CHECKPOINT_FIELDS),
            strict=True,
        ):
            validate_schema(kind, schema, fields)
        return {kind: "valid" for kind in ("events", "state", "artifact", "checkpoint")}

    async def append_event(self, draft: EventDraft) -> tuple[Event, bool]:
        async with self._append_lock:
            existing = await self.find_event_by_idempotency(
                draft.temp_application_id, draft.idempotency_key
            )
            if existing is not None:
                if existing.command_hash != draft.command_hash:
                    raise ContractError("IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_COMMAND")
                if self._next_sequence is not None:
                    self._next_sequence = max(
                        self._next_sequence, existing.global_sequence + 1
                    )
                return existing, False
            stream = await self.stream_events(draft.temp_application_id)
            stream_version = max((item.stream_version for item in stream), default=0) + 1
            if self._next_sequence is None:
                pages = await self.event_reader.query(
                    self.events_id,
                    sorts=[{"property": "global_sequence", "direction": "descending"}],
                    page_size=1,
                )
                current = int(decode_page(pages[0]).get("global_sequence") or 0) if pages else 0
                self._next_sequence = current + 1
            sequence = self._next_sequence
            event = Event(
                event_id=new_id("EVT"),
                global_sequence=sequence,
                event_type=draft.event_type,
                stream_version=stream_version,
                temp_application_id=draft.temp_application_id,
                occurred_at=draft.occurred_at,
                recorded_at=utc_text(),
                actor_type=draft.actor_type,
                actor_id=draft.actor_id,
                esm_owner_id_at_event_time=draft.esm_owner_id,
                esm_owner_display_name_at_event_time=draft.esm_owner_display_name,
                source_session_id=draft.source_session_id,
                execution_id=draft.execution_id,
                correlation_id=draft.correlation_id,
                causation_id=draft.causation_id,
                idempotency_key=draft.idempotency_key,
                command_hash=draft.command_hash,
                schema_version=PROJECTION_VERSION,
                payload=deepcopy(draft.payload),
                payload_hash=sha256_value(draft.payload),
                artifact_reference_ids=list(draft.artifact_reference_ids),
                target_event_id=draft.target_event_id,
            )
            values = event.as_record()
            values["application_id"] = None
            values["esm_owner_user_at_event_time"] = []
            try:
                created_page = await self.event_writer.create_page(
                    self.events_id, encode_values(EVENT_FIELDS, values)
                )
            except PersistenceError as exc:
                raise exc.with_context(
                    failed_phase="event_append",
                    event_id=event.event_id,
                ) from exc
            self._next_sequence = sequence + 1
            page_id = created_page.get("id")
            if not isinstance(page_id, str) or not page_id:
                raise PersistenceError(
                    "NOTION_RESPONSE_INVALID",
                    write_may_have_succeeded=True,
                    failed_phase="event_append",
                    event_id=event.event_id,
                    event_commit_acknowledged=None,
                )
            # A 2xx create response with a page ID is Notion's commit
            # acknowledgement. Some API responses do not immediately contain a
            # complete, independently decodable property set. The Event Reader and
            # ExpertState projection are therefore verified by the async Worker and
            # the bounded projection wait, rather than rejecting a committed write.
            return event, True

    async def _find_event_by_event_id(self, event_id: str) -> Event | None:
        pages = await self.event_reader.query(
            self.events_id,
            filter_body=equals_filter(EVENT_FIELDS, "event_id", event_id),
            page_size=2,
        )
        if len(pages) > 1:
            raise PersistenceError("NOTION_EVENT_ID_NOT_UNIQUE")
        return _decode_event(pages[0]) if pages else None

    async def find_event_by_idempotency(
        self, temp_application_id: str, idempotency_key: str
    ) -> Event | None:
        pages = await self.event_reader.query(
            self.events_id,
            filter_body={
                "and": [
                    equals_filter(EVENT_FIELDS, "temp_application_id", temp_application_id),
                    equals_filter(EVENT_FIELDS, "idempotency_key", idempotency_key),
                ]
            },
            page_size=2,
        )
        if len(pages) > 1:
            raise PersistenceError("NOTION_IDEMPOTENCY_NOT_UNIQUE")
        return _decode_event(pages[0]) if pages else None

    async def events_after(self, global_sequence: int, limit: int = 100) -> list[Event]:
        pages = await self.event_reader.query(
            self.events_id,
            filter_body={
                "property": "global_sequence",
                "number": {"greater_than": global_sequence},
            },
            sorts=[{"property": "global_sequence", "direction": "ascending"}],
            page_size=min(limit, 100),
        )
        return [_decode_event(page) for page in pages[:limit]]

    async def list_all_artifacts(self) -> list[Artifact]:
        pages = await self.agent_reader.query(self.artifact_id, page_size=100, fetch_all=True)
        return [_decode_artifact(page) for page in pages]

    async def stream_events(self, temp_application_id: str) -> list[Event]:
        pages = await self.event_reader.query(
            self.events_id,
            filter_body=equals_filter(
                EVENT_FIELDS, "temp_application_id", temp_application_id
            ),
            sorts=[{"property": "stream_version", "direction": "ascending"}],
            page_size=100,
            fetch_all=True,
        )
        return [_decode_event(page) for page in pages]

    async def get_state(self, temp_application_id: str) -> ExpertState | None:
        pages = await self.agent_reader.query(
            self.state_id,
            filter_body=equals_filter(
                STATE_FIELDS, "temp_application_id", temp_application_id
            ),
            page_size=2,
        )
        if len(pages) > 1:
            raise PersistenceError("NOTION_STATE_NOT_UNIQUE")
        return _decode_state(pages[0]) if pages else None

    async def find_states_by_exact_name(self, expert_name: str) -> list[ExpertState]:
        pages = await self.agent_reader.query(
            self.state_id,
            filter_body=equals_filter(STATE_FIELDS, "expert_name", expert_name.strip()),
            page_size=100,
            fetch_all=True,
        )
        return [_decode_state(page) for page in pages]

    async def find_state_by_public_profile_url(self, url: str) -> ExpertState | None:
        pages = await self.agent_reader.query(
            self.state_id,
            filter_body=equals_filter(STATE_FIELDS, "primary_public_profile_url", url),
            page_size=2,
        )
        if len(pages) > 1:
            raise PersistenceError("NOTION_PROFILE_URL_NOT_UNIQUE")
        return _decode_state(pages[0]) if pages else None

    async def public_profile_url_exists(self, url: str) -> bool:
        if await self.find_state_by_public_profile_url(url) is not None:
            return True
        pages = await self.event_reader.query(
            self.events_id,
            filter_body={"property": "payload", "rich_text": {"contains": url}},
            page_size=100,
            fetch_all=True,
        )
        return any(
            event.payload.get("primary_public_profile_url") == url
            for event in (_decode_event(page) for page in pages)
            if event.event_type
            in {"expert_profile_updated", "sourcing_assessment_recorded"}
        )

    async def upsert_state(self, state: ExpertState) -> None:
        pages = await self.agent_reader.query(
            self.state_id,
            filter_body=equals_filter(
                STATE_FIELDS, "temp_application_id", state.temp_application_id
            ),
            page_size=2,
        )
        if len(pages) > 1:
            raise PersistenceError("NOTION_STATE_NOT_UNIQUE")
        values = _state_record(state)
        properties = encode_values(STATE_FIELDS, values)
        if pages:
            page_id = pages[0].get("id")
            if not isinstance(page_id, str):
                raise PersistenceError("NOTION_RESPONSE_INVALID")
            await self.state_store.update_page(page_id, properties)
        else:
            await self.state_store.create_page(self.state_id, properties)

    async def put_artifact(self, artifact: Artifact) -> ArtifactWriteReceipt:
        acknowledged = self._artifact_commit_acks.get(artifact.artifact_id)
        if acknowledged is not None:
            if not artifacts_equivalent(acknowledged, artifact):
                raise ContractError("ARTIFACT_ID_REUSED_WITH_DIFFERENT_CONTENT")
            try:
                readback = await self.get_artifact(artifact.artifact_id)
                supersession_visible = await self._supersede_artifact_predecessor(
                    artifact
                )
            except PersistenceError as exc:
                logger.warning(
                    "artifact_commit_ack_readback_pending artifact_id=%s error_code=%s",
                    artifact.artifact_id,
                    exc.result_code,
                )
                readback = None
                supersession_visible = False
            if readback is not None and not artifacts_equivalent(artifact, readback):
                mismatch_fields = artifact_mismatch_fields(artifact, readback)
                logger.error(
                    "artifact_readback_mismatch artifact_id=%s mismatch_fields=%s",
                    artifact.artifact_id,
                    ",".join(mismatch_fields),
                )
                raise PersistenceError(
                    "ARTIFACT_READBACK_MISMATCH",
                    write_may_have_succeeded=True,
                    failed_phase="artifact_readback",
                    artifact_id=artifact.artifact_id,
                    artifact_commit_acknowledged=True,
                    artifact_readback_succeeded=False,
                    event_commit_acknowledged=False,
                )
            return ArtifactWriteReceipt(
                artifact=artifact,
                created=False,
                commit_acknowledged=True,
                readback_succeeded=(
                    readback is not None
                    and artifacts_equivalent(artifact, readback)
                    and supersession_visible
                ),
            )
        existing = await self.get_artifact(artifact.artifact_id)
        if existing is not None:
            if not artifacts_equivalent(artifact, existing):
                raise ContractError("ARTIFACT_ID_REUSED_WITH_DIFFERENT_CONTENT")
            supersession_visible = await self._supersede_artifact_predecessor(artifact)
            return ArtifactWriteReceipt(
                artifact=existing,
                created=False,
                commit_acknowledged=True,
                readback_succeeded=supersession_visible,
            )
        family = await self.find_artifacts(
            temp_application_id=artifact.temp_application_id,
            artifact_type=artifact.artifact_type,
        )
        if any(
            item.logical_artifact_id == artifact.logical_artifact_id
            and item.artifact_version == artifact.artifact_version
            for item in family
        ):
            raise ContractError("ARTIFACT_VERSION_CONFLICT")
        values = artifact.as_record()
        values["esm_owner_user"] = []
        values["source_artifact_ids"] = canonical_json(artifact.source_artifact_ids)
        created_page = await self.artifact_store.create_page(
            self.artifact_id, encode_values(ARTIFACT_FIELDS, values)
        )
        page_id = created_page.get("id")
        if not isinstance(page_id, str) or not page_id:
            raise PersistenceError(
                "NOTION_RESPONSE_INVALID",
                write_may_have_succeeded=True,
                failed_phase="artifact_create",
                artifact_id=artifact.artifact_id,
                artifact_commit_acknowledged=None,
                event_commit_acknowledged=False,
            )
        self._artifact_commit_acks[artifact.artifact_id] = artifact
        readback: Artifact | None = None
        try:
            for _attempt in range(3):
                readback = await self.get_artifact(artifact.artifact_id)
                if readback is not None:
                    break
                await asyncio.sleep(0.1)
        except PersistenceError as exc:
            logger.warning(
                "artifact_post_commit_readback_pending artifact_id=%s error_code=%s",
                artifact.artifact_id,
                exc.result_code,
            )
            readback = None
        if readback is not None and not artifacts_equivalent(artifact, readback):
            mismatch_fields = artifact_mismatch_fields(artifact, readback)
            logger.error(
                "artifact_readback_mismatch artifact_id=%s mismatch_fields=%s",
                artifact.artifact_id,
                ",".join(mismatch_fields),
            )
            raise PersistenceError(
                "ARTIFACT_READBACK_MISMATCH",
                write_may_have_succeeded=True,
                failed_phase="artifact_readback",
                artifact_id=artifact.artifact_id,
                artifact_commit_acknowledged=True,
                artifact_readback_succeeded=False,
                event_commit_acknowledged=False,
            )
        try:
            supersession_visible = await self._supersede_artifact_predecessor(artifact)
        except PersistenceError as exc:
            logger.warning(
                "artifact_supersession_readback_pending artifact_id=%s error_code=%s",
                artifact.artifact_id,
                exc.result_code,
            )
            supersession_visible = False
        return ArtifactWriteReceipt(
            artifact=artifact,
            created=True,
            commit_acknowledged=True,
            readback_succeeded=(
                readback is not None
                and artifacts_equivalent(artifact, readback)
                and supersession_visible
            ),
        )

    async def _supersede_artifact_predecessor(self, artifact: Artifact) -> bool:
        if not artifact.supersedes_artifact_id:
            return True
        predecessor = await self.get_artifact(artifact.supersedes_artifact_id)
        if (
            predecessor is None
            or predecessor.logical_artifact_id != artifact.logical_artifact_id
            or predecessor.artifact_version >= artifact.artifact_version
        ):
            raise ContractError("ARTIFACT_SUPERSEDES_INVALID")
        if predecessor.artifact_status == "superseded":
            return True
        if predecessor.artifact_status != "active":
            raise ContractError("ARTIFACT_SUPERSEDES_INVALID")
        pages = await self.agent_reader.query(
            self.artifact_id,
            filter_body=equals_filter(
                ARTIFACT_FIELDS, "artifact_id", predecessor.artifact_id
            ),
            page_size=2,
        )
        if len(pages) != 1 or not isinstance(pages[0].get("id"), str):
            raise PersistenceError("NOTION_ARTIFACT_NOT_UNIQUE")
        updated_page = await self.artifact_store.update_page(
            pages[0]["id"],
            {"artifact_status": encode_property("select", "superseded")},
        )
        if not isinstance(updated_page.get("id"), str):
            raise PersistenceError(
                "NOTION_RESPONSE_INVALID", write_may_have_succeeded=True
            )
        readback = await self.get_artifact(predecessor.artifact_id)
        if readback is None:
            return False
        if readback.artifact_status != "superseded":
            raise PersistenceError(
                "ARTIFACT_SUPERSEDE_READBACK_FAILED", write_may_have_succeeded=True
            )
        return True

    async def mark_artifact_orphaned(
        self, artifact_id: str, temp_application_id: str
    ) -> Artifact:
        artifact = await self.get_artifact(artifact_id)
        if artifact is None or artifact.temp_application_id != temp_application_id:
            raise ContractError("ARTIFACT_NOT_FOUND")
        if artifact.artifact_status != "active":
            raise ContractError("ARTIFACT_NOT_ACTIVE")
        pages = await self.agent_reader.query(
            self.artifact_id,
            filter_body=equals_filter(ARTIFACT_FIELDS, "artifact_id", artifact_id),
            page_size=2,
        )
        if len(pages) != 1 or not isinstance(pages[0].get("id"), str):
            raise PersistenceError("NOTION_ARTIFACT_NOT_UNIQUE")
        try:
            updated_page = await self.artifact_store.update_page(
                pages[0]["id"],
                {"artifact_status": encode_property("select", "orphaned")},
            )
        except PersistenceError as exc:
            raise exc.with_context(
                failed_phase="orphan_status_update",
                artifact_id=artifact_id,
            ) from exc
        if not isinstance(updated_page.get("id"), str):
            raise PersistenceError(
                "NOTION_RESPONSE_INVALID",
                write_may_have_succeeded=True,
                failed_phase="orphan_status_update",
                artifact_id=artifact_id,
                artifact_commit_acknowledged=None,
            )
        for _attempt in range(3):
            readback = await self.get_artifact(artifact_id)
            if readback is not None and readback.artifact_status == "orphaned":
                return readback
            await asyncio.sleep(0.1)
        raise PersistenceError(
            "ARTIFACT_ORPHAN_READBACK_FAILED",
            write_may_have_succeeded=True,
            failed_phase="orphan_status_readback",
            artifact_id=artifact_id,
            artifact_commit_acknowledged=True,
        )

    async def get_artifact(self, artifact_id: str) -> Artifact | None:
        pages = await self.agent_reader.query(
            self.artifact_id,
            filter_body=equals_filter(ARTIFACT_FIELDS, "artifact_id", artifact_id),
            page_size=2,
        )
        if len(pages) > 1:
            raise PersistenceError("NOTION_ARTIFACT_NOT_UNIQUE")
        return _decode_artifact(pages[0]) if pages else None

    async def find_artifacts(
        self,
        *,
        temp_application_id: str,
        artifact_type: str | None = None,
        version_role: str | None = None,
    ) -> list[Artifact]:
        filters = [
            equals_filter(
                ARTIFACT_FIELDS, "temp_application_id", temp_application_id
            )
        ]
        if artifact_type:
            filters.append(equals_filter(ARTIFACT_FIELDS, "artifact_type", artifact_type))
        if version_role:
            filters.append(equals_filter(ARTIFACT_FIELDS, "version_role", version_role))
        pages = await self.agent_reader.query(
            self.artifact_id,
            filter_body=filters[0] if len(filters) == 1 else {"and": filters},
            page_size=100,
            fetch_all=True,
        )
        return sorted(
            (_decode_artifact(page) for page in pages),
            key=lambda item: (item.logical_artifact_id, item.artifact_version),
        )

    async def get_checkpoint(self) -> int:
        pages = await self.checkpoint_store.query(
            self.checkpoint_id,
            filter_body=equals_filter(
                CHECKPOINT_FIELDS, "checkpoint_id", CHECKPOINT_ID
            ),
            page_size=2,
        )
        if len(pages) > 1:
            raise PersistenceError("NOTION_CHECKPOINT_NOT_UNIQUE")
        return int(decode_page(pages[0]).get("last_processed_global_sequence") or 0) if pages else 0

    async def set_checkpoint(self, global_sequence: int, event_id: str) -> None:
        pages = await self.checkpoint_store.query(
            self.checkpoint_id,
            filter_body=equals_filter(
                CHECKPOINT_FIELDS, "checkpoint_id", CHECKPOINT_ID
            ),
            page_size=2,
        )
        if len(pages) > 1:
            raise PersistenceError("NOTION_CHECKPOINT_NOT_UNIQUE")
        current = int(decode_page(pages[0]).get("last_processed_global_sequence") or 0) if pages else 0
        if global_sequence < current:
            raise ContractError("CHECKPOINT_REGRESSION")
        now = utc_text()
        values = {
            "checkpoint_id": CHECKPOINT_ID,
            "projector_name": "esm-v1-projector",
            "projector_version": PROJECTION_VERSION,
            "supported_event_schema_versions": ["1.0.0", "1.1.0", "1.2.0"],
            "shard_key": "all",
            "status": "idle",
            "run_mode": "incremental",
            "last_processed_global_sequence": global_sequence,
            "last_processed_event_id": event_id,
            "last_successful_checkpoint_at": now,
            "current_run_started_at": None,
            "current_run_completed_at": now,
            "replay_from_global_sequence": None,
            "replay_temp_application_id": None,
            "lease_owner": None,
            "lease_expires_at": None,
            "events_scanned_last_run": 1,
            "streams_succeeded_last_run": 1,
            "streams_failed_last_run": 0,
            "dead_letter_count": 0,
            "projection_lag_seconds": 0,
            "last_error_code": None,
            "last_error_summary": None,
            "updated_at": now,
        }
        encoded = encode_values(CHECKPOINT_FIELDS, values)
        if pages:
            page_id = pages[0].get("id")
            if not isinstance(page_id, str):
                raise PersistenceError("NOTION_RESPONSE_INVALID")
            await self.checkpoint_store.update_page(page_id, encoded)
        else:
            await self.checkpoint_store.create_page(self.checkpoint_id, encoded)


def _decode_event(page: dict[str, Any]) -> Event:
    values = decode_page(page)
    try:
        payload = json.loads(values.get("payload") or "{}")
        artifact_ids = json.loads(values.get("artifact_reference_ids") or "[]")
    except (TypeError, json.JSONDecodeError) as exc:
        raise PersistenceError("NOTION_EVENT_JSON_INVALID") from exc
    return Event(
        event_id=values["event_id"],
        global_sequence=int(values["global_sequence"]),
        event_type=values["event_type"],
        stream_version=int(values["stream_version"]),
        temp_application_id=values["temp_application_id"],
        occurred_at=values["occurred_at"],
        recorded_at=values["recorded_at"],
        actor_type=values["actor_type"],
        actor_id=values["actor_id"],
        esm_owner_id_at_event_time=values["esm_owner_id_at_event_time"],
        esm_owner_display_name_at_event_time=values["esm_owner_display_name_at_event_time"],
        source_session_id=values.get("source_session_id") or "",
        execution_id=values.get("execution_id") or None,
        correlation_id=values["correlation_id"],
        causation_id=values.get("causation_id") or None,
        idempotency_key=values["idempotency_key"],
        command_hash=values["command_hash"],
        schema_version=values["schema_version"],
        payload=payload,
        payload_hash=values["payload_hash"],
        artifact_reference_ids=artifact_ids,
        target_event_id=values.get("target_event_id") or None,
    )


def _decode_state(page: dict[str, Any]) -> ExpertState:
    values = decode_page(page)
    base_names = {
        "temp_application_id", "expert_name", "source", "esm_owner_id",
        "esm_owner_display_name", "current_stage", "stage_status", "stage_outcome",
        "professional_domains", "market_regions", "next_action",
        "requires_esm_confirmation", "loop_count", "failure_count",
        "last_applied_event_id", "last_processed_stream_version",
        "last_processed_global_sequence", "projection_conflict", "projection_conflict_code",
    }
    internal_values = {
        key: value
        for key, value in values.items()
        if key not in base_names
        and not key.startswith("_")
        and key not in {"projection_version", "projection_updated_at", "state_hash", "esm_owner_user"}
        and value not in (None, "")
    }
    for name in ("discovery_source_artifact_ids", "positioning_source_artifact_ids"):
        raw = internal_values.get(name)
        if isinstance(raw, str):
            try:
                internal_values[name] = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise PersistenceError("NOTION_STATE_JSON_INVALID") from exc
    return ExpertState(
        temp_application_id=values["temp_application_id"],
        expert_name=values["expert_name"],
        source=values["source"],
        esm_owner_id=values["esm_owner_id"],
        esm_owner_display_name=values["esm_owner_display_name"],
        current_stage=values["current_stage"],
        stage_status=values["stage_status"],
        stage_outcome=values.get("stage_outcome") or None,
        professional_domains=values.get("professional_domains") or [],
        market_regions=values.get("market_regions") or [],
        next_action=values.get("next_action") or None,
        requires_esm_confirmation=bool(values.get("requires_esm_confirmation")),
        loop_count=int(values.get("loop_count") or 0),
        failure_count=int(values.get("failure_count") or 0),
        last_applied_event_id=values.get("last_applied_event_id") or None,
        last_processed_stream_version=int(values.get("last_processed_stream_version") or 0),
        last_processed_global_sequence=int(values.get("last_processed_global_sequence") or 0),
        projection_conflict=bool(values.get("projection_conflict")),
        projection_conflict_code=values.get("projection_conflict_code") or None,
        values=internal_values,
    )


def _state_record(state: ExpertState) -> dict[str, Any]:
    values = state.as_record()
    values.setdefault("application_id", None)
    values["esm_owner_user"] = []
    values.setdefault("requires_reactivation", False)
    values.setdefault("outreach_status", "not_started")
    values.setdefault("discovery_source_registered", False)
    values.setdefault("positioning_source_registered", False)
    values.setdefault("positioning_meeting_count", 0)
    values.setdefault("expert_conversation_access_status", "not_provided")
    values.setdefault("expert_agent_access_status", "not_provided")
    for name in ("discovery_source_artifact_ids", "positioning_source_artifact_ids"):
        values[name] = canonical_json(values.get(name) or [])
    values["dropped_stage"] = state.current_stage if state.stage_status in {"closed", "canceled"} else None
    values["projection_version"] = PROJECTION_VERSION
    values["projection_updated_at"] = utc_text()
    hash_input = {
        key: value
        for key, value in values.items()
        if key not in {"projection_version", "projection_updated_at", "state_hash", "esm_owner_user"}
    }
    values["state_hash"] = sha256_value(hash_input)
    return values


def _decode_artifact(page: dict[str, Any]) -> Artifact:
    values = decode_page(page)
    try:
        source_ids = json.loads(values.get("source_artifact_ids") or "[]")
    except (TypeError, json.JSONDecodeError) as exc:
        raise PersistenceError("NOTION_ARTIFACT_JSON_INVALID") from exc
    return Artifact(
        artifact_id=values["artifact_id"],
        logical_artifact_id=values["logical_artifact_id"],
        artifact_version=int(values["artifact_version"]),
        artifact_schema_version=values["artifact_schema_version"],
        artifact_type=values["artifact_type"],
        artifact_kind=values["artifact_kind"],
        version_role=values["version_role"],
        artifact_status=values["artifact_status"],
        source_execution_id=values.get("source_execution_id") or None,
        source_result_hash=values.get("source_result_hash") or None,
        application_id=values.get("application_id") or None,
        temp_application_id=values["temp_application_id"],
        expert_name_snapshot=values["expert_name_snapshot"],
        esm_owner_id=values["esm_owner_id"],
        esm_owner_display_name=values["esm_owner_display_name"],
        stage=values["stage"],
        display_title=values["display_title"],
        storage_type=values["storage_type"],
        content_page_or_object_id=values.get("content_page_or_object_id") or None,
        controlled_access_url=values["controlled_access_url"],
        mime_type=values.get("mime_type") or None,
        size_bytes=int(values["size_bytes"]) if values.get("size_bytes") is not None else None,
        language=values.get("language") or [],
        readable_content_descriptor=values.get("readable_content_descriptor") or None,
        content_hash=values["content_hash"],
        created_at=values["created_at"],
        created_by_actor_type=values["created_by_actor_type"],
        created_by_actor_id=values["created_by_actor_id"],
        reviewed_at=values.get("reviewed_at") or None,
        reviewed_by_actor_id=values.get("reviewed_by_actor_id") or None,
        supersedes_artifact_id=values.get("supersedes_artifact_id") or None,
        source_artifact_ids=source_ids,
        access_policy=values["access_policy"],
        allowed_purposes=values.get("allowed_purposes") or [],
        link_access_status=values["link_access_status"],
        link_last_checked_at=values.get("link_last_checked_at") or None,
        ingestion_status=values["ingestion_status"],
        security_scan_status=values["security_scan_status"],
        retention_until=values.get("retention_until") or None,
    )


NOTION_ENV_NAMES = (
    "NOTION_EVENT_READER_TOKEN",
    "NOTION_EVENT_WRITER_TOKEN",
    "NOTION_STATE_STORE_TOKEN",
    "NOTION_ARTIFACT_STORE_TOKEN",
    "NOTION_CHECKPOINT_STORE_TOKEN",
    "NOTION_ESM_AGENT_READER_TOKEN",
    "NOTION_EVENTS_DATA_SOURCE_ID",
    "NOTION_EXPERT_STATE_DATA_SOURCE_ID",
    "NOTION_ARTIFACT_INDEX_DATA_SOURCE_ID",
    "NOTION_PROJECTION_CHECKPOINTS_DATA_SOURCE_ID",
)


def storage_from_environment() -> NotionStorage:
    values = {name: os.environ.get(name, "").strip() for name in NOTION_ENV_NAMES}
    if not all(values.values()):
        raise ConfigurationError("NOTION_CONFIGURATION_MISSING")
    return NotionStorage(
        event_reader_token=values["NOTION_EVENT_READER_TOKEN"],
        event_writer_token=values["NOTION_EVENT_WRITER_TOKEN"],
        state_store_token=values["NOTION_STATE_STORE_TOKEN"],
        artifact_store_token=values["NOTION_ARTIFACT_STORE_TOKEN"],
        checkpoint_store_token=values["NOTION_CHECKPOINT_STORE_TOKEN"],
        esm_agent_reader_token=values["NOTION_ESM_AGENT_READER_TOKEN"],
        events_data_source_id=values["NOTION_EVENTS_DATA_SOURCE_ID"],
        state_data_source_id=values["NOTION_EXPERT_STATE_DATA_SOURCE_ID"],
        artifact_data_source_id=values["NOTION_ARTIFACT_INDEX_DATA_SOURCE_ID"],
        checkpoint_data_source_id=values["NOTION_PROJECTION_CHECKPOINTS_DATA_SOURCE_ID"],
    )
