"""Frozen V1 command, Event, Artifact, and state contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .errors import ContractError
from .util import canonical_json, sha256_value


CURRENT_STAGES = frozenset(
    {
        "sourcing",
        "evaluation",
        "initial_outreach",
        "discovery_call",
        "agent_positioning",
        "onboarding_handoff",
    }
)
STAGE_STATUSES = frozenset(
    {
        "ready",
        "in_progress",
        "ai_draft_ready",
        "awaiting_esm_review",
        "reviewed_final_ready",
        "prep_ready",
        "prep_in_progress",
        "prep_ai_draft_ready",
        "awaiting_prep_review",
        "ready_to_send",
        "awaiting_reply",
        "reply_received",
        "awaiting_schedule",
        "ready_for_call",
        "call_completed_awaiting_source",
        "recap_ready",
        "recap_in_progress",
        "recap_ai_draft_ready",
        "awaiting_recap_review",
        "paused",
        "failed_retryable",
        "intervention_required",
        "canceled",
        "closed",
        "completed",
    }
)
SELF_ONBOARDING_VALUES = frozenset({"yes", "no", "unsure"})
PIPELINE_TRACK_VALUES = frozenset({"expert_led", "esm_led", "hybrid"})
SOURCE_VALUES = frozenset({"inbound", "esm_outbound"})
PROFESSIONAL_DOMAIN_VALUES = frozenset(
    {
        "marketing",
        "sales",
        "gtm",
        "international_trade",
        "cross_border_ecommerce",
        "product",
        "operations",
        "people_hr",
        "finance",
        "legal",
        "healthcare",
        "technology",
        "education",
        "creator_economy",
        "other",
    }
)
MARKET_REGION_VALUES = frozenset(
    {"na", "latam", "emea", "apac", "china_mainland", "global", "other"}
)
OUTREACH_CHANNELS = frozenset(
    {"email", "linkedin", "x", "whatsapp", "wechat", "phone", "other"}
)
RESPONSE_CATEGORIES = frozenset({"positive", "neutral", "negative", "no_decision"})
V1_DISABLED_OUTCOMES = frozenset(
    {
        ("evaluation", "pause"),
        ("initial_outreach", "pause"),
        ("agent_positioning", "pause"),
    }
)
TERMINAL_OUTCOMES = frozenset(
    {
        ("sourcing", "do_not_enter"),
        ("evaluation", "reject"),
        ("initial_outreach", "reject"),
        ("discovery_call", "reject"),
        ("agent_positioning", "reject"),
    }
)

ARTIFACT_TYPES: dict[str, frozenset[str]] = {
    "public_profile_reference": frozenset({"source"}),
    "sourcing_report": frozenset({"ai_draft", "human_supplement", "reviewed_final"}),
    "evaluation_report": frozenset({"ai_draft", "human_supplement", "reviewed_final"}),
    "initial_outreach_material": frozenset(
        {"ai_draft", "human_supplement", "reviewed_final"}
    ),
    "outreach_response": frozenset({"source"}),
    "discovery_call_prep": frozenset(
        {"ai_draft", "human_supplement", "reviewed_final"}
    ),
    "discovery_call_transcript": frozenset({"source"}),
    "discovery_call_notes": frozenset({"source"}),
    "discovery_call_recap": frozenset(
        {"ai_draft", "human_supplement", "reviewed_final"}
    ),
    "expert_conversation_link": frozenset({"source"}),
    "expert_agent_link": frozenset({"source"}),
    "positioning_call_prep": frozenset(
        {"ai_draft", "human_supplement", "reviewed_final"}
    ),
    "positioning_call_transcript": frozenset({"source"}),
    "positioning_call_notes": frozenset({"source"}),
    "positioning_call_recap": frozenset(
        {"ai_draft", "human_supplement", "reviewed_final"}
    ),
    "esm_work_note": frozenset({"human_supplement"}),
}
ARTIFACT_KIND_VALUES = frozenset(
    {"work_product", "reference_source", "external_reference", "operational"}
)
ARTIFACT_STATUS_VALUES = frozenset(
    {"active", "superseded", "voided", "orphaned", "quarantined"}
)
ACCESS_POLICY_VALUES = frozenset({"owner_only", "task_scoped"})
ALLOWED_PURPOSE_VALUES = frozenset(
    {
        "sourcing",
        "evaluation",
        "outreach",
        "discovery_prep",
        "discovery_recap",
        "positioning_prep",
        "positioning_recap",
        "onboarding_handoff",
    }
)

STAGE_ARTIFACT_TYPES: dict[str, frozenset[str]] = {
    "sourcing": frozenset({"public_profile_reference", "sourcing_report", "esm_work_note"}),
    "evaluation": frozenset({"evaluation_report", "esm_work_note"}),
    "initial_outreach": frozenset(
        {"initial_outreach_material", "outreach_response", "esm_work_note"}
    ),
    "discovery_call": frozenset(
        {
            "discovery_call_prep",
            "discovery_call_transcript",
            "discovery_call_notes",
            "discovery_call_recap",
            "esm_work_note",
        }
    ),
    "agent_positioning": frozenset(
        {
            "expert_conversation_link",
            "expert_agent_link",
            "positioning_call_prep",
            "positioning_call_transcript",
            "positioning_call_notes",
            "positioning_call_recap",
            "esm_work_note",
        }
    ),
    "onboarding_handoff": frozenset({"esm_work_note"}),
}

EVENT_PAYLOAD_FIELDS: dict[str, tuple[frozenset[str], frozenset[str]]] = {
    "application_created": (
        frozenset(
            {
                "expert_name",
                "source",
                "professional_domains",
                "market_regions",
                "entry_path",
            }
        ),
        frozenset(
            {
                "sourcing_run_id",
                "skipped_stages",
                "trusted_entry_reason_short",
            }
        ),
    ),
    "sourcing_assessment_recorded": (
        frozenset({"identity_confidence", "sourcing_priority", "candidate_readiness"}),
        frozenset({"primary_public_profile_url"}),
    ),
    "expert_profile_updated": (
        frozenset({"reason"}),
        frozenset(
            {
                "expert_name",
                "professional_domains",
                "market_regions",
                "primary_public_profile_url",
            }
        ),
    ),
    "stage_processing_started": (
        frozenset({"stage", "operation_id", "attempt"}),
        frozenset(),
    ),
    "stage_ai_draft_generated": (
        frozenset({"stage", "artifact_type", "artifact_id", "artifact_version"}),
        frozenset(),
    ),
    "stage_review_requested": (
        frozenset({"stage", "artifact_id"}),
        frozenset(),
    ),
    "stage_reviewed_final_confirmed": (
        frozenset(
            {"stage", "artifact_type", "artifact_id", "artifact_version", "reviewed_at"}
        ),
        frozenset(),
    ),
    "call_prep_review_completed": (
        frozenset({"stage", "reviewed_at"}),
        frozenset({"artifact_id"}),
    ),
    "sourcing_outcome_confirmed": (
        frozenset({"outcome", "decided_at"}),
        frozenset(),
    ),
    "evaluation_outcome_confirmed": (
        frozenset({"outcome", "decided_at"}),
        frozenset({"evaluation_summary_short"}),
    ),
    "outreach_sent": (
        frozenset({"channel", "sent_at", "artifact_id"}),
        frozenset(),
    ),
    "outreach_response_recorded": (
        frozenset({"received_at", "response_category"}),
        frozenset({"response_summary_short", "artifact_id"}),
    ),
    "outreach_outcome_confirmed": (
        frozenset({"outcome"}),
        frozenset({"reason_code"}),
    ),
    "discovery_call_scheduled": (
        frozenset({"scheduled_at"}),
        frozenset({"calendar_reference"}),
    ),
    "discovery_call_completed": (frozenset({"completed_at"}), frozenset()),
    "discovery_source_registered": (
        frozenset({"artifact_id", "source_type"}),
        frozenset(),
    ),
    "intervention_requested": (
        frozenset({"reason_code", "next_action"}),
        frozenset(),
    ),
    "discovery_outcome_confirmed": (
        frozenset(
            {
                "outcome",
                "decided_at",
                "self_onboarding_capable",
                "pipeline_track",
                "next_action",
            }
        ),
        frozenset(),
    ),
    "positioning_call_scheduled": (
        frozenset({"scheduled_at"}),
        frozenset({"calendar_reference"}),
    ),
    "positioning_call_completed": (frozenset({"completed_at"}), frozenset()),
    "positioning_source_registered": (
        frozenset({"artifact_id", "source_type"}),
        frozenset(),
    ),
    "positioning_outcome_confirmed": (
        frozenset({"outcome", "decided_at", "next_action"}),
        frozenset({"first_kit_direction_short"}),
    ),
    "event_corrected": (
        frozenset(
            {
                "correction_mode",
                "reason",
                "corrected_event_type",
                "corrected_payload",
                "target_event_hash",
                "confirmation_id",
            }
        ),
        frozenset(),
    ),
}


def require_short(value: Any, field_name: str, *, maximum: int = 280) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ContractError("COMMAND_PAYLOAD_INVALID", f"Invalid {field_name}.")
    return value.strip()


def validate_event_payload(event_type: str, payload: dict[str, Any]) -> None:
    spec = EVENT_PAYLOAD_FIELDS.get(event_type)
    if spec is None:
        raise ContractError("EVENT_TYPE_UNSUPPORTED_V1")
    required, optional = spec
    keys = set(payload)
    if not required.issubset(keys) or keys - required - optional:
        raise ContractError("COMMAND_PAYLOAD_INVALID")
    if event_type == "application_created":
        require_short(payload.get("expert_name"), "expert_name")
        if payload.get("source") not in SOURCE_VALUES:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if payload.get("entry_path") not in {"standard", "trusted_direct"}:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        for field_name in ("professional_domains", "market_regions"):
            if not isinstance(payload.get(field_name), list):
                raise ContractError("COMMAND_PAYLOAD_INVALID")
        if not set(payload["professional_domains"]).issubset(PROFESSIONAL_DOMAIN_VALUES):
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if not set(payload["market_regions"]).issubset(MARKET_REGION_VALUES):
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if payload["entry_path"] == "trusted_direct":
            if payload.get("skipped_stages") != ["sourcing", "evaluation"]:
                raise ContractError("COMMAND_PAYLOAD_INVALID")
            require_short(payload.get("trusted_entry_reason_short"), "trusted_entry_reason_short")
        elif "skipped_stages" in payload or "trusted_entry_reason_short" in payload:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
    elif event_type == "expert_profile_updated":
        if not any(
            field_name in payload
            for field_name in (
                "expert_name",
                "professional_domains",
                "market_regions",
                "primary_public_profile_url",
            )
        ):
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if "professional_domains" in payload and not set(
            payload["professional_domains"]
        ).issubset(PROFESSIONAL_DOMAIN_VALUES):
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if "market_regions" in payload and not set(payload["market_regions"]).issubset(
            MARKET_REGION_VALUES
        ):
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if "expert_name" in payload:
            require_short(payload["expert_name"], "expert_name")
        if "primary_public_profile_url" in payload:
            _require_http_url(payload["primary_public_profile_url"])
    elif event_type == "sourcing_assessment_recorded":
        if payload.get("identity_confidence") not in {"high", "medium", "low", "unknown"}:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if payload.get("sourcing_priority") not in {"p0", "p1", "hold"}:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if payload.get("candidate_readiness") not in {
            "ready",
            "needs_disambiguation",
            "low_public_surface",
        }:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if "primary_public_profile_url" in payload:
            _require_http_url(payload["primary_public_profile_url"])
    elif event_type == "stage_processing_started":
        if payload.get("stage") not in CURRENT_STAGES:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if not isinstance(payload.get("attempt"), int) or payload["attempt"] < 1:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        require_short(payload.get("operation_id"), "operation_id")
    elif event_type in {
        "stage_ai_draft_generated",
        "stage_review_requested",
        "stage_reviewed_final_confirmed",
    }:
        if payload.get("stage") not in CURRENT_STAGES:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
    elif event_type == "discovery_outcome_confirmed":
        if payload.get("outcome") not in {"proceed", "re_evaluate", "reject"}:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if payload.get("self_onboarding_capable") not in SELF_ONBOARDING_VALUES:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if payload.get("pipeline_track") not in PIPELINE_TRACK_VALUES:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        require_short(payload.get("next_action"), "next_action", maximum=500)
    elif event_type == "evaluation_outcome_confirmed":
        if payload.get("outcome") not in {"advance", "reject"}:
            raise ContractError("OUTCOME_UNSUPPORTED_V1")
        if "evaluation_summary_short" in payload:
            require_short(payload["evaluation_summary_short"], "evaluation_summary_short")
    elif event_type == "outreach_outcome_confirmed":
        if payload.get("outcome") != "reject":
            raise ContractError("OUTCOME_UNSUPPORTED_V1")
    elif event_type == "positioning_outcome_confirmed":
        if payload.get("outcome") not in {"advance_to_onboarding", "rework", "reject"}:
            raise ContractError("OUTCOME_UNSUPPORTED_V1")
        require_short(payload.get("next_action"), "next_action", maximum=500)
        if payload.get("outcome") == "advance_to_onboarding":
            require_short(payload.get("first_kit_direction_short"), "first_kit_direction_short")
    elif event_type == "sourcing_outcome_confirmed":
        if payload.get("outcome") not in {"enter_evaluation", "do_not_enter"}:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
    elif event_type == "outreach_sent":
        if payload.get("channel") not in OUTREACH_CHANNELS:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
    elif event_type in {"discovery_source_registered", "positioning_source_registered"}:
        if payload.get("source_type") not in {"transcript", "notes"}:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
    elif event_type == "outreach_response_recorded":
        if payload.get("response_category") not in RESPONSE_CATEGORIES:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if "response_summary_short" in payload:
            require_short(payload["response_summary_short"], "response_summary_short")
    elif event_type == "intervention_requested":
        require_short(payload.get("reason_code"), "reason_code")
        require_short(payload.get("next_action"), "next_action")
    elif event_type == "event_corrected":
        if payload.get("correction_mode") != "self_service_no_downstream":
            raise ContractError("CORRECTION_MODE_UNSUPPORTED_V1")
        require_short(payload.get("reason"), "reason")
        if not isinstance(payload.get("corrected_payload"), dict):
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        require_short(payload.get("corrected_event_type"), "corrected_event_type")
        require_short(payload.get("target_event_hash"), "target_event_hash")
        require_short(payload.get("confirmation_id"), "confirmation_id")


def _require_http_url(value: Any) -> None:
    if not isinstance(value, str) or not value.startswith(("https://", "http://")):
        raise ContractError("COMMAND_PAYLOAD_INVALID")


@dataclass(frozen=True, slots=True)
class Event:
    event_id: str
    global_sequence: int
    event_type: str
    stream_version: int
    temp_application_id: str
    occurred_at: str
    recorded_at: str
    actor_type: str
    actor_id: str
    esm_owner_id_at_event_time: str
    esm_owner_display_name_at_event_time: str
    source_session_id: str
    execution_id: str | None
    correlation_id: str
    causation_id: str | None
    idempotency_key: str
    command_hash: str
    schema_version: str
    payload: dict[str, Any]
    payload_hash: str
    artifact_reference_ids: list[str] = field(default_factory=list)
    target_event_id: str | None = None

    def __post_init__(self) -> None:
        validate_event_payload(self.event_type, self.payload)
        if self.payload_hash != sha256_value(self.payload):
            raise ContractError("EVENT_PAYLOAD_HASH_MISMATCH")
        if self.actor_type not in {"esm", "agent", "system", "integration", "admin"}:
            raise ContractError("EVENT_ACTOR_INVALID")
        payload_artifact_ids = {
            value
            for key, value in self.payload.items()
            if key == "artifact_id" and isinstance(value, str)
        }
        if not payload_artifact_ids.issubset(set(self.artifact_reference_ids)):
            raise ContractError("EVENT_ARTIFACT_REFERENCE_MISMATCH")
        if self.event_type == "event_corrected" and not self.target_event_id:
            raise ContractError("CORRECTION_TARGET_REQUIRED")

    def as_record(self) -> dict[str, Any]:
        result = {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
            if name not in {"payload", "artifact_reference_ids"}
        }
        result["payload"] = canonical_json(self.payload)
        result["artifact_reference_ids"] = canonical_json(self.artifact_reference_ids)
        return result


@dataclass(slots=True)
class ExpertState:
    temp_application_id: str
    expert_name: str
    source: str
    esm_owner_id: str
    esm_owner_display_name: str
    current_stage: str
    stage_status: str
    stage_outcome: str | None = None
    professional_domains: list[str] = field(default_factory=list)
    market_regions: list[str] = field(default_factory=list)
    next_action: str | None = None
    requires_esm_confirmation: bool = False
    loop_count: int = 0
    failure_count: int = 0
    last_applied_event_id: str | None = None
    last_processed_stream_version: int = 0
    last_processed_global_sequence: int = 0
    projection_conflict: bool = False
    projection_conflict_code: str | None = None
    values: dict[str, Any] = field(default_factory=dict)

    def as_record(self) -> dict[str, Any]:
        base = {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
            if name != "values"
        }
        base.update(self.values)
        return base


@dataclass(frozen=True, slots=True)
class Artifact:
    artifact_id: str
    logical_artifact_id: str
    artifact_version: int
    artifact_schema_version: str
    artifact_type: str
    artifact_kind: str
    version_role: str
    artifact_status: str
    source_execution_id: str | None
    source_result_hash: str | None
    application_id: str | None
    temp_application_id: str
    expert_name_snapshot: str
    esm_owner_id: str
    esm_owner_display_name: str
    stage: str
    display_title: str
    storage_type: str
    content_page_or_object_id: str | None
    controlled_access_url: str
    mime_type: str | None
    size_bytes: int | None
    language: list[str]
    readable_content_descriptor: str | None
    content_hash: str
    created_at: str
    created_by_actor_type: str
    created_by_actor_id: str
    reviewed_at: str | None = None
    reviewed_by_actor_id: str | None = None
    supersedes_artifact_id: str | None = None
    source_artifact_ids: list[str] = field(default_factory=list)
    access_policy: str = "owner_only"
    allowed_purposes: list[str] = field(default_factory=list)
    link_access_status: str = "not_checked"
    link_last_checked_at: str | None = None
    ingestion_status: str = "ready"
    security_scan_status: str = "not_scanned"
    retention_until: str | None = None

    def __post_init__(self) -> None:
        roles = ARTIFACT_TYPES.get(self.artifact_type)
        if roles is None or self.version_role not in roles:
            raise ContractError("ARTIFACT_CONTRACT_INVALID")
        if self.artifact_kind not in ARTIFACT_KIND_VALUES:
            raise ContractError("ARTIFACT_CONTRACT_INVALID")
        if self.artifact_status not in ARTIFACT_STATUS_VALUES:
            raise ContractError("ARTIFACT_CONTRACT_INVALID")
        if self.access_policy not in ACCESS_POLICY_VALUES:
            raise ContractError("ARTIFACT_CONTRACT_INVALID")
        if not set(self.allowed_purposes).issubset(ALLOWED_PURPOSE_VALUES):
            raise ContractError("ARTIFACT_CONTRACT_INVALID")
        if self.ingestion_status not in {"pending", "ready", "failed", "quarantined"}:
            raise ContractError("ARTIFACT_CONTRACT_INVALID")
        if self.security_scan_status not in {
            "not_scanned",
            "passed",
            "flagged",
            "not_applicable",
        }:
            raise ContractError("ARTIFACT_CONTRACT_INVALID")
        if self.size_bytes is not None and self.size_bytes < 0:
            raise ContractError("ARTIFACT_CONTRACT_INVALID")
        if self.stage not in CURRENT_STAGES or self.artifact_type not in STAGE_ARTIFACT_TYPES[self.stage]:
            raise ContractError("ARTIFACT_CONTRACT_INVALID")
        if self.version_role == "reviewed_final":
            if not self.reviewed_at or not self.reviewed_by_actor_id:
                raise ContractError("ARTIFACT_REVIEW_METADATA_REQUIRED")
        if self.link_access_status != "not_checked":
            raise ContractError("LINK_ACCESS_STATUS_UNSUPPORTED_V1")
        if self.storage_type != "controlled_external_url":
            raise ContractError("ARTIFACT_STORAGE_TYPE_UNSUPPORTED_V1")
        if self.content_page_or_object_id is not None:
            raise ContractError("ARTIFACT_OBJECT_ID_UNSUPPORTED_V1")
        if self.artifact_schema_version != "1.1.0":
            raise ContractError("ARTIFACT_SCHEMA_VERSION_UNSUPPORTED")
        if self.version_role == "ai_draft" and (
            not self.source_execution_id or not self.source_result_hash
        ):
            raise ContractError("ARTIFACT_SOURCE_BINDING_REQUIRED")
        if self.version_role == "reviewed_final" and not self.source_execution_id:
            raise ContractError("ARTIFACT_SOURCE_BINDING_REQUIRED")
        if self.readable_content_descriptor is not None and len(self.readable_content_descriptor) > 280:
            raise ContractError("ARTIFACT_COMMENT_TOO_LONG")
        if not self.controlled_access_url.startswith("https://"):
            raise ContractError("ARTIFACT_URL_INVALID")

    def as_record(self) -> dict[str, Any]:
        return {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
        }
