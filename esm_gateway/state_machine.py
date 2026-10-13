"""Deterministic V1 reducer with product-scope overrides."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

from .contracts import Event, ExpertState
from .errors import StateTransitionError
from .util import sha256_value


ADMIN_REJECT_REASON = "agent_reject_recommendation_requires_admin_review"
ADMIN_REVIEW_NEXT_ACTION = "请 ESM Admin 审核 Agent 的 reject 建议"


def _require(state: ExpertState, stage: str, statuses: set[str]) -> None:
    if state.current_stage != stage or state.stage_status not in statuses:
        raise StateTransitionError("INVALID_STATE_TRANSITION")
    if state.stage_status in {"closed", "canceled", "completed"}:
        raise StateTransitionError("APPLICATION_TERMINAL_V1")


def _processing_target(stage: str, status: str) -> str:
    mapping = {
        ("sourcing", "ready"): "in_progress",
        ("evaluation", "ready"): "in_progress",
        ("initial_outreach", "ready"): "prep_in_progress",
        ("discovery_call", "prep_ready"): "prep_in_progress",
        ("discovery_call", "recap_ready"): "recap_in_progress",
        ("agent_positioning", "prep_ready"): "prep_in_progress",
        ("agent_positioning", "recap_ready"): "recap_in_progress",
    }
    target = mapping.get((stage, status))
    if target is None:
        raise StateTransitionError("INVALID_STATE_TRANSITION")
    return target


def _draft_target(status: str) -> str:
    mapping = {
        "in_progress": "ai_draft_ready",
        "prep_in_progress": "prep_ai_draft_ready",
        "recap_in_progress": "recap_ai_draft_ready",
    }
    target = mapping.get(status)
    if target is None:
        raise StateTransitionError("INVALID_STATE_TRANSITION")
    return target


def _review_target(status: str) -> str:
    mapping = {
        "ai_draft_ready": "awaiting_esm_review",
        "prep_ai_draft_ready": "awaiting_prep_review",
        "recap_ai_draft_ready": "awaiting_recap_review",
    }
    target = mapping.get(status)
    if target is None:
        raise StateTransitionError("INVALID_STATE_TRANSITION")
    return target


def reduce_event(state: ExpertState | None, event: Event) -> ExpertState:
    payload = event.payload
    if event.event_type == "application_created":
        if state is not None or event.stream_version != 1:
            raise StateTransitionError("APPLICATION_ALREADY_EXISTS")
        trusted = payload["entry_path"] == "trusted_direct"
        created = ExpertState(
            temp_application_id=event.temp_application_id,
            expert_name=payload["expert_name"],
            source=payload["source"],
            esm_owner_id=event.esm_owner_id_at_event_time,
            esm_owner_display_name=event.esm_owner_display_name_at_event_time,
            current_stage="initial_outreach" if trusted else "sourcing",
            stage_status="ready",
            professional_domains=list(payload["professional_domains"]),
            market_regions=list(payload["market_regions"]),
            values={
                "application_id": None,
                "outreach_status": "not_started",
                "sourcing_run_id": payload.get("sourcing_run_id"),
                "requires_reactivation": False,
                "discovery_source_registered": False,
                "positioning_source_registered": False,
                "discovery_source_artifact_ids": [],
                "positioning_source_artifact_ids": [],
                "expert_conversation_access_status": "not_provided",
                "expert_agent_access_status": "not_provided",
                "positioning_meeting_count": 0,
                "created_at": event.occurred_at,
            },
        )
        if payload["entry_path"] == "csv_import":
            created.current_stage = "agent_positioning"
            created.stage_status = "awaiting_progress_assessment"
            created.next_action = "Owner 核对资料并确认正常排期"
            created.requires_esm_confirmation = True
            created.values["primary_public_profile_url"] = payload["primary_public_profile_url"]
            created.values["expert_agent_artifact_id"] = payload["expert_agent_artifact_id"]
            created.values["expert_agent_access_status"] = "not_checked" if payload["expert_agent_artifact_id"] else "not_provided"
        return _finish(created, event)
    if state is None:
        raise StateTransitionError("APPLICATION_NOT_FOUND")
    if event.temp_application_id != state.temp_application_id:
        raise StateTransitionError("STREAM_ID_MISMATCH")
    if event.stream_version != state.last_processed_stream_version + 1:
        raise StateTransitionError("STREAM_VERSION_CONFLICT")
    if event.esm_owner_id_at_event_time != state.esm_owner_id:
        raise StateTransitionError("OWNER_CONTEXT_MISMATCH")

    result = deepcopy(state)
    event_type = event.event_type
    if event_type == "sourcing_assessment_recorded":
        _require(result, "sourcing", {"ready", "in_progress", "ai_draft_ready", "awaiting_esm_review", "reviewed_final_ready"})
        result.values.update(payload)
    elif event_type == "expert_profile_updated":
        if result.stage_status in {"closed", "canceled", "completed"}:
            raise StateTransitionError("APPLICATION_TERMINAL_V1")
        if "expert_name" in payload:
            result.expert_name = str(payload["expert_name"]).strip()
        if "professional_domains" in payload:
            result.professional_domains = list(payload["professional_domains"])
        if "market_regions" in payload:
            result.market_regions = list(payload["market_regions"])
        if "primary_public_profile_url" in payload:
            result.values["primary_public_profile_url"] = payload["primary_public_profile_url"]
    elif event_type == "stage_processing_started":
        if payload["stage"] != result.current_stage:
            raise StateTransitionError("INVALID_STATE_TRANSITION")
        result.stage_status = _processing_target(result.current_stage, result.stage_status)
    elif event_type == "stage_ai_draft_generated":
        if payload["stage"] != result.current_stage:
            raise StateTransitionError("INVALID_STATE_TRANSITION")
        result.stage_status = _draft_target(result.stage_status)
    elif event_type == "stage_review_requested":
        if payload["stage"] != result.current_stage:
            raise StateTransitionError("INVALID_STATE_TRANSITION")
        result.stage_status = _review_target(result.stage_status)
        result.requires_esm_confirmation = True
    elif event_type == "stage_reviewed_final_confirmed":
        if payload["stage"] != result.current_stage:
            raise StateTransitionError("INVALID_STATE_TRANSITION")
        _apply_reviewed_final(
            result, payload["artifact_type"], str(payload["artifact_id"])
        )
        result.requires_esm_confirmation = False
    elif event_type == "call_prep_review_completed":
        _require(result, "agent_positioning", {"awaiting_prep_review"})
        result.stage_status = "ready_for_call"
        result.requires_esm_confirmation = False
    elif event_type == "sourcing_outcome_confirmed":
        _require(result, "sourcing", {"reviewed_final_ready"})
        _require_gate(result, "sourcing_final_artifact_id")
        outcome = payload["outcome"]
        result.values["sourcing_decision"] = outcome
        result.values["sourcing_decided_at"] = payload["decided_at"]
        if outcome == "enter_evaluation":
            _enter(result, "evaluation", "ready")
        else:
            _close(result, outcome)
    elif event_type == "evaluation_outcome_confirmed":
        _require(result, "evaluation", {"reviewed_final_ready"})
        _require_gate(result, "evaluation_final_artifact_id")
        outcome = payload["outcome"]
        result.values["screening_result"] = outcome
        result.values["screening_decided_at"] = payload["decided_at"]
        if "evaluation_summary_short" in payload:
            result.values["evaluation_summary_short"] = payload["evaluation_summary_short"]
        if outcome == "advance":
            _enter(result, "initial_outreach", "ready")
        else:
            _close(result, outcome)
    elif event_type == "outreach_sent":
        _require(result, "initial_outreach", {"ready_to_send"})
        result.stage_status = "awaiting_reply"
        result.values.update(
            {
                "outreach_channel": payload["channel"],
                "outreach_sent_at": payload["sent_at"],
                "outreach_status": "awaiting_reply",
            }
        )
    elif event_type == "outreach_response_recorded":
        _require(result, "initial_outreach", {"awaiting_reply"})
        result.stage_status = "reply_received"
        result.values["outreach_status"] = "replied"
        result.values["outreach_response_received_at"] = payload["received_at"]
        result.values["outreach_response_category"] = payload["response_category"]
        if "response_summary_short" in payload:
            result.values["outreach_response_summary_short"] = payload["response_summary_short"]
    elif event_type == "outreach_outcome_confirmed":
        _require(result, "initial_outreach", {"ready", "ready_to_send", "awaiting_reply", "reply_received"})
        _close(result, "reject")
        result.values["outreach_status"] = "rejected"
    elif event_type == "discovery_call_scheduled":
        _require(result, "initial_outreach", {"awaiting_reply", "reply_received"})
        result.values["discovery_scheduled_at"] = payload["scheduled_at"]
        result.values["outreach_status"] = "discovery_scheduled"
        _enter(result, "discovery_call", "prep_ready")
    elif event_type == "discovery_call_completed":
        _require(result, "discovery_call", {"ready_for_call"})
        result.stage_status = "call_completed_awaiting_source"
        result.values["discovery_completed_at"] = payload["completed_at"]
    elif event_type == "discovery_source_registered":
        _require(result, "discovery_call", {"call_completed_awaiting_source"})
        result.stage_status = "recap_ready"
        result.values["discovery_source_registered"] = True
        result.values.setdefault("discovery_source_artifact_ids", []).append(
            payload["artifact_id"]
        )
    elif event_type == "intervention_requested":
        _require(result, "discovery_call", {"reviewed_final_ready"})
        if payload["reason_code"] != ADMIN_REJECT_REASON:
            raise StateTransitionError("INTERVENTION_UNSUPPORTED_V1")
        if payload["next_action"] != ADMIN_REVIEW_NEXT_ACTION:
            raise StateTransitionError("INTERVENTION_UNSUPPORTED_V1")
        result.stage_status = "intervention_required"
        result.stage_outcome = None
        result.requires_esm_confirmation = True
        result.next_action = payload["next_action"]
    elif event_type == "discovery_outcome_confirmed":
        _apply_discovery_outcome(result, payload, event.artifact_reference_ids)
    elif event_type == "positioning_progress_assessed":
        _require(result, "agent_positioning", {"awaiting_progress_assessment"})
        if event.actor_id != result.esm_owner_id or event.actor_type not in {"esm", "admin"}:
            raise StateTransitionError("EXPERT_WRITE_FORBIDDEN_OWNER_MISMATCH")
        _enter(result, "agent_positioning", "awaiting_schedule")
        result.next_action = "联系专家安排 Positioning Call"
    elif event_type == "positioning_call_scheduled":
        _require(result, "agent_positioning", {"awaiting_schedule"})
        result.values["positioning_scheduled_at"] = payload["scheduled_at"]
        result.stage_status = "prep_ready"
    elif event_type == "positioning_call_completed":
        _require(result, "agent_positioning", {"ready_for_call"})
        result.stage_status = "call_completed_awaiting_source"
        result.values["positioning_completed_at"] = payload["completed_at"]
        result.values["positioning_meeting_count"] = int(result.values.get("positioning_meeting_count", 0)) + 1
    elif event_type == "positioning_source_registered":
        _require(result, "agent_positioning", {"call_completed_awaiting_source"})
        result.stage_status = "recap_ready"
        result.values["positioning_source_registered"] = True
        result.values.setdefault("positioning_source_artifact_ids", []).append(
            payload["artifact_id"]
        )
    elif event_type == "positioning_outcome_confirmed":
        _apply_positioning_outcome(result, payload)
    else:
        raise StateTransitionError("EVENT_TYPE_UNSUPPORTED_V1")
    return _finish(result, event)


def replay_stream(events: list[Event]) -> ExpertState:
    """Rebuild one stream, applying V1 correction Events as replacements."""

    if not events:
        raise StateTransitionError("APPLICATION_NOT_FOUND")
    by_id = {event.event_id: event for event in events}
    overrides: dict[str, Event] = {}
    for correction in events:
        if correction.event_type != "event_corrected":
            continue
        target = by_id.get(correction.target_event_id or "")
        if target is None or target.event_type == "event_corrected":
            raise StateTransitionError("CORRECTION_TARGET_INVALID")
        if correction.payload["target_event_hash"] != sha256_value(target.as_record()):
            raise StateTransitionError("CORRECTION_TARGET_HASH_MISMATCH")
        if correction.payload["corrected_event_type"] != target.event_type:
            raise StateTransitionError("CORRECTION_EVENT_TYPE_CHANGE_UNSUPPORTED_V1")
        corrected_payload = correction.payload["corrected_payload"]
        overrides[target.event_id] = replace(
            target,
            payload=deepcopy(corrected_payload),
            payload_hash=sha256_value(corrected_payload),
        )

    state: ExpertState | None = None
    for event in events:
        if event.event_type == "event_corrected":
            if state is None:
                raise StateTransitionError("CORRECTION_TARGET_INVALID")
            state.last_applied_event_id = event.event_id
            state.last_processed_stream_version = event.stream_version
            state.last_processed_global_sequence = event.global_sequence
            state.values["updated_at"] = event.occurred_at
            state.values["last_meaningful_action_at"] = event.occurred_at
            state.values["last_actor_id"] = event.actor_id
            continue
        state = reduce_event(state, overrides.get(event.event_id, event))
    if state is None:
        raise StateTransitionError("APPLICATION_NOT_FOUND")
    return state


def _apply_reviewed_final(
    state: ExpertState, artifact_type: str, artifact_id: str
) -> None:
    if state.stage_status == "awaiting_esm_review" and state.current_stage in {"sourcing", "evaluation"}:
        state.stage_status = "reviewed_final_ready"
    elif state.stage_status == "awaiting_prep_review":
        if state.current_stage == "initial_outreach":
            state.stage_status = "ready_to_send"
            state.values["outreach_status"] = "prepared"
        elif state.current_stage == "discovery_call":
            state.stage_status = "ready_for_call"
        else:
            raise StateTransitionError("INVALID_STATE_TRANSITION")
    elif state.stage_status == "awaiting_recap_review" and state.current_stage in {"discovery_call", "agent_positioning"}:
        state.stage_status = "reviewed_final_ready"
    else:
        raise StateTransitionError("INVALID_STATE_TRANSITION")
    state.values[f"{artifact_type}_reviewed_final"] = True
    gate_fields = {
        "sourcing_report": "sourcing_final_artifact_id",
        "evaluation_report": "evaluation_final_artifact_id",
        "initial_outreach_material": "outreach_final_artifact_id",
        "discovery_call_prep": "discovery_prep_final_artifact_id",
        "discovery_call_recap": "discovery_recap_final_artifact_id",
        "positioning_call_prep": "positioning_prep_final_artifact_id",
        "positioning_call_recap": "positioning_recap_final_artifact_id",
    }
    field_name = gate_fields.get(artifact_type)
    if field_name is not None:
        state.values[field_name] = artifact_id


def _apply_discovery_outcome(
    state: ExpertState,
    payload: dict[str, object],
    artifact_reference_ids: list[str],
) -> None:
    special = state.stage_status == "intervention_required"
    if special:
        if state.next_action != ADMIN_REVIEW_NEXT_ACTION or not artifact_reference_ids:
            raise StateTransitionError("ADMIN_REVIEW_REQUIRED")
    else:
        _require(state, "discovery_call", {"reviewed_final_ready"})
    _require_gate(state, "discovery_recap_final_artifact_id")
    outcome = str(payload["outcome"])
    if outcome == "reject" and not special:
        raise StateTransitionError("ADMIN_REVIEW_REQUIRED")
    state.values["discovery_outcome"] = outcome
    state.values["discovery_decided_at"] = payload["decided_at"]
    state.values["self_onboarding_capable"] = payload["self_onboarding_capable"]
    state.values["pipeline_track"] = payload["pipeline_track"]
    state.next_action = str(payload["next_action"])
    state.requires_esm_confirmation = False
    if outcome == "proceed":
        _enter(state, "agent_positioning", "awaiting_schedule")
    elif outcome == "re_evaluate":
        _enter(state, "evaluation", "ready")
        state.next_action = "重新执行 Evaluation"
    else:
        _close(state, "reject")


def _apply_positioning_outcome(state: ExpertState, payload: dict[str, object]) -> None:
    _require(state, "agent_positioning", {"reviewed_final_ready"})
    _require_gate(state, "positioning_recap_final_artifact_id")
    outcome = str(payload["outcome"])
    state.values["positioning_outcome"] = outcome
    state.values["positioning_decided_at"] = payload["decided_at"]
    state.next_action = str(payload["next_action"])
    if "first_kit_direction_short" in payload:
        state.values["first_kit_direction_short"] = payload["first_kit_direction_short"]
    if outcome == "advance_to_onboarding":
        _enter(state, "onboarding_handoff", "ready")
        state.next_action = "等待 Onboarding 接手"
    elif outcome == "rework":
        state.stage_status = "awaiting_schedule"
        state.stage_outcome = None
        state.loop_count += 1
    else:
        _close(state, "reject")


def _enter(state: ExpertState, stage: str, status: str) -> None:
    state.current_stage = stage
    state.stage_status = status
    state.stage_outcome = None
    state.requires_esm_confirmation = False


def _close(state: ExpertState, outcome: str) -> None:
    state.stage_status = "closed"
    state.stage_outcome = outcome
    state.requires_esm_confirmation = False


def _require_gate(state: ExpertState, field_name: str) -> None:
    if not state.values.get(field_name):
        raise StateTransitionError("REQUIRED_REVIEWED_FINAL_ARTIFACT_MISSING")


def _finish(state: ExpertState, event: Event) -> ExpertState:
    state.last_applied_event_id = event.event_id
    state.last_processed_stream_version = event.stream_version
    state.last_processed_global_sequence = event.global_sequence
    state.values["updated_at"] = event.occurred_at
    state.values["last_meaningful_action_at"] = event.occurred_at
    state.values["last_actor_id"] = event.actor_id
    return state
