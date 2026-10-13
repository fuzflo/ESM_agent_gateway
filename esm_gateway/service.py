"""Application service for identity, snapshots, prepare/confirm, and writes."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import asdict
from typing import Any

from .confirmation import CONFIRMATION_PHRASE, ConfirmationService
from .csv_service import CSVImportService
from .owner_snapshot import extend_owner_snapshot
from .contracts import (
    MARKET_REGION_VALUES,
    PROFESSIONAL_DOMAIN_VALUES,
    TERMINAL_OUTCOMES,
    Artifact,
    Event,
    require_short,
    validate_event_payload,
)
from .errors import (
    AuthorizationError,
    ContractError,
    GatewayError,
    PersistenceError,
    StateTransitionError,
)
from .identity import Identity, IdentityDirectory
from .projector import ProjectionWorker
from .state_machine import (
    ADMIN_REVIEW_NEXT_ACTION,
    reduce_event,
    replay_stream,
)
from .storage import EventDraft, Storage, artifacts_equivalent
from .util import (
    new_id,
    new_temp_application_id,
    sha256_value,
    utc_minute_text,
    utc_text,
)


SUPPORTED_COMMAND_TYPES = frozenset(
    {"create_standard_batch", "create_trusted_batch", "business_event", "admin_review", "import_experts_csv_batch", "retry_csv_import_batch"}
)
CORRECTABLE_EVENT_TYPES = frozenset(
    {
        "expert_profile_updated",
        "sourcing_assessment_recorded",
        "outreach_response_recorded",
        "discovery_call_scheduled",
        "discovery_call_completed",
        "positioning_call_scheduled",
        "positioning_call_completed",
    }
)
SKILL_EVENT_TYPES: dict[str, frozenset[str]] = {
    "expert-ta-sourcing-assistant": frozenset(
        {
            "expert_profile_updated",
            "sourcing_assessment_recorded",
            "stage_processing_started",
            "stage_ai_draft_generated",
            "stage_review_requested",
            "stage_reviewed_final_confirmed",
            "sourcing_outcome_confirmed",
            "event_corrected",
        }
    ),
    "expert-evaluation": frozenset(
        {
            "stage_processing_started",
            "stage_ai_draft_generated",
            "stage_review_requested",
            "stage_reviewed_final_confirmed",
            "evaluation_outcome_confirmed",
            "event_corrected",
        }
    ),
    "expert-initial-outreach": frozenset(
        {
            "stage_processing_started",
            "stage_ai_draft_generated",
            "stage_review_requested",
            "stage_reviewed_final_confirmed",
            "outreach_sent",
            "outreach_response_recorded",
            "outreach_outcome_confirmed",
            "discovery_call_scheduled",
            "discovery_call_completed",
            "discovery_source_registered",
            "intervention_requested",
            "discovery_outcome_confirmed",
            "event_corrected",
        }
    ),
    "expert-positioning": frozenset(
        {
            "positioning_progress_assessed",
            "stage_processing_started",
            "stage_ai_draft_generated",
            "stage_review_requested",
            "stage_reviewed_final_confirmed",
            "call_prep_review_completed",
            "positioning_call_scheduled",
            "positioning_call_completed",
            "positioning_source_registered",
            "positioning_outcome_confirmed",
            "event_corrected",
        }
    ),
}


class ESMGatewayService(CSVImportService):
    def __init__(
        self,
        *,
        identities: IdentityDirectory,
        confirmations: ConfirmationService,
        storage: Storage,
        projector: ProjectionWorker,
        projection_wait_seconds: float = 90,
    ) -> None:
        self.identities = identities
        self.confirmations = confirmations
        self.storage = storage
        self.projector = projector
        self.projection_wait_seconds = projection_wait_seconds
        self._write_lock = asyncio.Lock()
        self._csv_batches: dict[str, Any] = {}

    def verify_identity(self, raw_code: str) -> dict[str, Any]:
        identity = self.identities.verify(raw_code)
        if identity is None:
            return self._failure("IDENTITY_DENIED", identity_verified=False)
        return {
            "status": "success",
            "result_code": "ESM_IDENTITY_VERIFIED",
            "identity_verified": True,
            "identity_id": identity.esm_owner_id,
            "identity_display_name": identity.display_name,
            "identity_role": identity.role,
            "credential_material_returned": False,
        }

    def get_esm_roster(self, raw_code: str) -> dict[str, Any]:
        identity = self._require_identity(raw_code)
        if not identity.is_admin:
            raise AuthorizationError("ADMIN_ROLE_REQUIRED")
        return self._success(
            "ESM_ROSTER_RETRIEVED",
            identity,
            roster=self.identities.public_roster(),
        )

    async def get_snapshot(
        self,
        *,
        raw_code: str,
        expert_name: str,
        temp_application_id: str | None = None,
    ) -> dict[str, Any]:
        identity = self._require_identity(raw_code)
        name = expert_name.strip()
        if not name:
            raise ContractError("EXPERT_NAME_REQUIRED")
        matches = await self.storage.find_states_by_exact_name(name)
        if temp_application_id:
            matches = [
                state
                for state in matches
                if state.temp_application_id == temp_application_id.strip()
            ]
        if not matches:
            return self._success("EXPERT_NOT_FOUND", identity, snapshot=None)
        if len(matches) > 1:
            return self._success(
                "EXPERT_NAME_AMBIGUOUS_REQUIRE_TEMP_ID",
                identity,
                snapshot=None,
            )
        state = matches[0]
        if state.esm_owner_id != identity.esm_owner_id:
            if identity.is_admin:
                raise AuthorizationError("ADMIN_CROSS_OWNER_QUERY_NOT_AVAILABLE_IN_AGENT")
            snapshot = {
                "expert_name": state.expert_name,
                "temp_application_id": state.temp_application_id,
                "current_stage": state.current_stage,
                "stage_status": state.stage_status,
                "stage_outcome": state.stage_outcome,
            }
            return self._success("CROSS_OWNER_LIMITED_SNAPSHOT", identity, snapshot=snapshot)
        record = state.as_record()
        allowed = {
            "esm_owner_id", "esm_owner_display_name", "primary_public_profile_url",
            "expert_name",
            "temp_application_id",
            "source",
            "professional_domains",
            "market_regions",
            "current_stage",
            "stage_status",
            "stage_outcome",
            "next_action",
            "requires_esm_confirmation",
            "loop_count",
            "sourcing_decision",
            "sourcing_decided_at",
            "screening_result",
            "screening_decided_at",
            "evaluation_summary_short",
            "outreach_status",
            "outreach_channel",
            "outreach_sent_at",
            "outreach_response_received_at",
            "outreach_response_category",
            "outreach_response_summary_short",
            "discovery_scheduled_at",
            "discovery_completed_at",
            "discovery_outcome",
            "discovery_decided_at",
            "self_onboarding_capable",
            "pipeline_track",
            "positioning_scheduled_at",
            "positioning_completed_at",
            "positioning_meeting_count",
            "positioning_outcome",
            "positioning_decided_at",
            "first_kit_direction_short",
        }
        snapshot = {key: value for key, value in record.items() if key in allowed}
        await extend_owner_snapshot(self, state, snapshot)
        return self._success("OWNER_FULL_SNAPSHOT", identity, snapshot=snapshot)

    async def prepare_write(
        self,
        *,
        raw_code: str,
        source_session_id: str,
        command: dict[str, Any],
    ) -> dict[str, Any]:
        identity = self._require_identity(raw_code)
        normalized, warnings = await self._normalize_command(identity, command)
        command_type = normalized["command_type"]
        skill = normalized["skill"]
        subject = normalized["subject"]
        confirmation_id = new_id("CONF")
        if normalized.get("event_type") == "event_corrected":
            normalized["payload"]["confirmation_id"] = confirmation_id
        card = self.confirmations.issue(
            source_session_id=source_session_id,
            identity_id=identity.esm_owner_id,
            skill=skill,
            action=command_type,
            subject=subject,
            command=normalized,
            confirmation_id=confirmation_id,
        )
        terminal_warning = normalized.get("terminal_warning")
        return {
            "status": "success",
            "result_code": "WRITE_CONFIRMATION_CARD_ISSUED",
            "identity_verified": True,
            "confirmation_token": card.token,
            "confirmation_id": card.confirmation_id,
            "expires_at": card.expires_at,
            "confirmation_phrase": CONFIRMATION_PHRASE,
            "confirmation_notice": "写入确认卡有效期为10分钟。",
            "terminal_warning": terminal_warning,
            "preview": _public_preview(normalized),
            "warnings": warnings,
            "credential_material_returned": False,
        }

    async def confirm_write(
        self,
        *,
        raw_code: str,
        source_session_id: str,
        confirmation_token: str,
        confirmation_phrase: str,
    ) -> dict[str, Any]:
        identity = self._require_identity(raw_code)
        claims = self.confirmations.verify(
            token=confirmation_token, phrase=confirmation_phrase,
            identity_id=identity.esm_owner_id, source_session_id=source_session_id,
        )
        if claims["command"]["command_type"] in {"import_experts_csv_batch", "retry_csv_import_batch"}:
            return await self._confirm_csv(identity, source_session_id, claims)
        async with self._write_lock:
            claims = self.confirmations.verify(
                token=confirmation_token,
                phrase=confirmation_phrase,
                identity_id=identity.esm_owner_id,
                source_session_id=source_session_id,
            )
            command = claims["command"]
            command_type = command["command_type"]
            if command_type in {"create_standard_batch", "create_trusted_batch"}:
                result = await self._confirm_batch(identity, source_session_id, claims)
            elif command_type == "business_event":
                result = await self._confirm_business_event(
                    identity, source_session_id, claims
                )
            elif command_type == "admin_review":
                result = await self._confirm_admin_review(identity, claims)
            else:
                raise ContractError("COMMAND_TYPE_UNSUPPORTED_V1")
            self.confirmations.mark_consumed(
                source_session_id, str(claims["confirmation_id"])
            )
            return result

    async def _normalize_command(
        self, identity: Identity, command: dict[str, Any]
    ) -> tuple[dict[str, Any], list[str]]:
        if not isinstance(command, dict) or command.get("command_type") not in SUPPORTED_COMMAND_TYPES:
            raise ContractError("COMMAND_TYPE_UNSUPPORTED_V1")
        command_type = command["command_type"]
        if command_type == "import_experts_csv_batch":
            return await self._normalize_csv(identity, command)
        if command_type == "retry_csv_import_batch":
            return await self._normalize_csv_retry(identity, command)
        if command_type in {"create_standard_batch", "create_trusted_batch"}:
            return await self._normalize_batch(identity, command)
        if command_type == "business_event":
            return await self._normalize_business_event(identity, command)
        return await self._normalize_admin_review(identity, command)

    async def _normalize_batch(
        self, identity: Identity, command: dict[str, Any]
    ) -> tuple[dict[str, Any], list[str]]:
        _reject_unknown_fields(command, {"command_type", "batch_id", "candidates"})
        command_type = command["command_type"]
        if command_type == "create_trusted_batch" and not identity.is_admin:
            raise AuthorizationError("ADMIN_ROLE_REQUIRED")
        candidates = command.get("candidates")
        if not isinstance(candidates, list) or not candidates:
            raise ContractError("BATCH_CANDIDATES_REQUIRED")
        supplied_batch_id = _optional_text(command.get("batch_id"))
        if supplied_batch_id and not _is_prefixed_uuid(supplied_batch_id, "BATCH"):
            raise ContractError("BATCH_ID_INVALID")
        batch_id = supplied_batch_id or new_id("BATCH")
        warnings: list[str] = []
        normalized_candidates: list[dict[str, Any]] = []
        batch_profile_urls: set[str] = set()
        batch_names: set[str] = set()
        for raw in candidates:
            if not isinstance(raw, dict):
                raise ContractError("COMMAND_PAYLOAD_INVALID")
            common_fields = {
                "temp_application_id",
                "expert_name",
                "source",
                "professional_domains",
                "market_regions",
                "primary_public_profile_url",
            }
            trusted_fields = {"target_esm_owner_id", "trusted_entry_reason_short"}
            _reject_unknown_fields(
                raw,
                common_fields | trusted_fields
                if command_type == "create_trusted_batch"
                else common_fields,
            )
            expert_name = _required_text(raw.get("expert_name"), "expert_name")
            source = raw.get("source", "esm_outbound")
            if source not in {"inbound", "esm_outbound"}:
                raise ContractError("COMMAND_PAYLOAD_INVALID")
            professional_domains = _frozen_enum_list(
                raw.get("professional_domains", []),
                allowed=PROFESSIONAL_DOMAIN_VALUES,
                field_name="professional_domains",
            )
            market_regions = _frozen_enum_list(
                raw.get("market_regions", []),
                allowed=MARKET_REGION_VALUES,
                field_name="market_regions",
            )
            supplied_temp_id = _optional_text(raw.get("temp_application_id"))
            temp_id = supplied_temp_id or new_temp_application_id()
            if supplied_temp_id and not _is_temp_application_id(supplied_temp_id):
                raise ContractError("TEMP_APPLICATION_ID_INVALID")
            existing_events = await self.storage.stream_events(temp_id)
            profile_url = _optional_text(raw.get("primary_public_profile_url"))
            if profile_url:
                same_expert_already_has_url = any(
                    item.event_type
                    in {"expert_profile_updated", "sourcing_assessment_recorded"}
                    and item.payload.get("primary_public_profile_url") == profile_url
                    for item in existing_events
                )
                if profile_url in batch_profile_urls or (
                    not same_expert_already_has_url
                    and await self.storage.public_profile_url_exists(profile_url)
                ):
                    raise ContractError("DUPLICATE_PUBLIC_PROFILE_URL")
                batch_profile_urls.add(profile_url)
            same_names = await self.storage.find_states_by_exact_name(expert_name)
            if expert_name in batch_names or any(
                item.temp_application_id != temp_id for item in same_names
            ):
                warnings.append(f"同名警告：{expert_name}")
            batch_names.add(expert_name)
            if command_type == "create_trusted_batch":
                owner_id = _required_text(raw.get("target_esm_owner_id"), "target_esm_owner_id")
                owner = self.identities.require_owner(owner_id)
                reason = _required_text(
                    raw.get("trusted_entry_reason_short"),
                    "trusted_entry_reason_short",
                    maximum=280,
                )
                entry_path = "trusted_direct"
            else:
                owner = identity
                reason = None
                entry_path = "standard"
            created_payload = _application_created_payload(
                expert_name=expert_name,
                source=source,
                professional_domains=professional_domains,
                market_regions=market_regions,
                entry_path=entry_path,
                trusted_entry_reason_short=reason,
            )
            validate_event_payload("application_created", created_payload)
            if profile_url:
                validate_event_payload(
                    "expert_profile_updated",
                    {
                        "reason": "initial_public_profile_registration",
                        "primary_public_profile_url": profile_url,
                    },
                )
            if existing_events:
                created = existing_events[0]
                if (
                    created.event_type != "application_created"
                    or created.esm_owner_id_at_event_time != owner.esm_owner_id
                    or created.payload != created_payload
                ):
                    raise ContractError("TEMP_APPLICATION_ID_ALREADY_IN_USE")
                warnings.append(f"已提交项将安全回读，不会重复创建：{expert_name}")
            normalized_candidates.append(
                {
                    "temp_application_id": temp_id,
                    "expert_name": expert_name,
                    "source": source,
                    "professional_domains": professional_domains,
                    "market_regions": market_regions,
                    "primary_public_profile_url": profile_url,
                    "entry_path": entry_path,
                    "trusted_entry_reason_short": reason,
                    "owner_id": owner.esm_owner_id,
                    "owner_display_name": owner.display_name,
                }
            )
        normalized = {
            "command_type": command_type,
            "skill": "trusted-direct-entry" if command_type == "create_trusted_batch" else "expert-ta-sourcing-assistant",
            "subject": batch_id,
            "batch_id": batch_id,
            "candidates": normalized_candidates,
        }
        return normalized, warnings

    async def _normalize_business_event(
        self, identity: Identity, command: dict[str, Any]
    ) -> tuple[dict[str, Any], list[str]]:
        _reject_unknown_fields(
            command,
            {
                "command_type",
                "skill",
                "temp_application_id",
                "expert_name",
                "event_type",
                "payload",
                "artifact",
                "reference_artifact_ids",
                "target_event_id",
            },
        )
        temp_id = _required_text(command.get("temp_application_id"), "temp_application_id")
        expert_name = _required_text(command.get("expert_name"), "expert_name")
        state = await self.storage.get_state(temp_id)
        if state is None or state.expert_name.strip() != expert_name:
            raise ContractError("EXPERT_NOT_FOUND")
        if state.esm_owner_id != identity.esm_owner_id:
            raise AuthorizationError("EXPERT_WRITE_FORBIDDEN_OWNER_MISMATCH")
        if state.stage_status in {"closed", "canceled", "completed"}:
            raise StateTransitionError("APPLICATION_TERMINAL_V1")
        event_type = _required_text(command.get("event_type"), "event_type")
        if event_type == "positioning_progress_assessed" and (
            command.get("artifact") is not None or command.get("reference_artifact_ids")
            or command.get("target_event_id") is not None
        ):
            raise ContractError("POSITIONING_REVIEW_METADATA_FORBIDDEN")
        skill = _required_text(command.get("skill"), "skill")
        if event_type not in SKILL_EVENT_TYPES.get(skill, frozenset()):
            raise AuthorizationError("SKILL_EVENT_COMBINATION_FORBIDDEN")
        payload = deepcopy(command.get("payload"))
        if not isinstance(payload, dict):
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        target_event_id: str | None = None
        correction_diff: dict[str, Any] | None = None
        if event_type == "event_corrected":
            target_event_id, payload, correction_diff = await self._normalize_correction(
                state, command, payload
            )
        artifact_input = command.get("artifact")
        artifact: Artifact | None = None
        if artifact_input is not None:
            artifact = await self._normalize_artifact(identity, state, artifact_input)
            payload = _bind_artifact_payload(event_type, payload, artifact)
        validate_event_payload(event_type, payload)
        reference_artifact_ids = _string_list(command.get("reference_artifact_ids", []))
        if event_type == "discovery_outcome_confirmed" and state.stage_status == "intervention_required":
            if len(reference_artifact_ids) != 1:
                raise ContractError("ADMIN_REVIEW_REQUIRED")
            review = await self.storage.get_artifact(reference_artifact_ids[0])
            if review is None or review.artifact_type != "esm_work_note" or review.stage != "discovery_call":
                raise ContractError("ADMIN_REVIEW_REQUIRED")
            if review.temp_application_id != state.temp_application_id:
                raise ContractError("ADMIN_REVIEW_REQUIRED")
            creator = self.identities.get_by_id(review.created_by_actor_id)
            if creator is None or not creator.is_admin:
                raise ContractError("ADMIN_REVIEW_REQUIRED")
        await self._preview_transition(
            state,
            identity,
            event_type,
            payload,
            artifact,
            reference_artifact_ids,
            target_event_id,
        )
        terminal_warning = None
        outcome = payload.get("outcome")
        if (state.current_stage, outcome) in TERMINAL_OUTCOMES:
            terminal_warning = "此操作将关闭该专家；V1 不支持重新开启。"
        normalized = {
            "command_type": "business_event",
            "skill": skill,
            "subject": temp_id,
            "temp_application_id": temp_id,
            "expert_name": expert_name,
            "event_type": event_type,
            "payload": payload,
            "artifact": asdict(artifact) if artifact else None,
            "reference_artifact_ids": reference_artifact_ids,
            "target_event_id": target_event_id,
            "correction_diff": correction_diff,
            "expected_stream_version": state.last_processed_stream_version,
            "terminal_warning": terminal_warning,
        }
        return normalized, []

    async def _normalize_correction(
        self,
        state: Any,
        command: dict[str, Any],
        payload: dict[str, Any],
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        if command.get("artifact") is not None or command.get("reference_artifact_ids"):
            raise ContractError("CORRECTION_ARTIFACT_CHANGE_UNSUPPORTED_V1")
        target_event_id = _required_text(
            command.get("target_event_id"), "target_event_id"
        )
        if set(payload) != {
            "correction_mode",
            "reason",
            "corrected_event_type",
            "corrected_payload",
        }:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        if payload.get("correction_mode") != "self_service_no_downstream":
            raise ContractError("CORRECTION_MODE_UNSUPPORTED_V1")
        require_short(payload.get("reason"), "reason")
        stream = await self.storage.stream_events(state.temp_application_id)
        if not stream or stream[-1].event_id != target_event_id:
            raise ContractError("CORRECTION_HAS_DOWNSTREAM_DEPENDENCIES")
        target = stream[-1]
        corrected_event_type = payload.get("corrected_event_type")
        if (
            corrected_event_type != target.event_type
            or corrected_event_type not in CORRECTABLE_EVENT_TYPES
        ):
            raise ContractError("CORRECTION_EVENT_TYPE_UNSUPPORTED_V1")
        corrected_payload = payload.get("corrected_payload")
        if not isinstance(corrected_payload, dict):
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        validate_event_payload(corrected_event_type, corrected_payload)
        return (
            target_event_id,
            {
                **payload,
                "target_event_hash": sha256_value(target.as_record()),
                "confirmation_id": "PENDING_CONFIRMATION_ID",
            },
            {"before": deepcopy(target.payload), "after": deepcopy(corrected_payload)},
        )

    async def _normalize_admin_review(
        self, identity: Identity, command: dict[str, Any]
    ) -> tuple[dict[str, Any], list[str]]:
        _reject_unknown_fields(
            command,
            {
                "command_type",
                "temp_application_id",
                "expert_name",
                "display_title",
                "controlled_access_url",
                "readable_content_descriptor",
            },
        )
        if not identity.is_admin:
            raise AuthorizationError("ADMIN_ROLE_REQUIRED")
        temp_id = _required_text(command.get("temp_application_id"), "temp_application_id")
        expert_name = _required_text(command.get("expert_name"), "expert_name")
        state = await self.storage.get_state(temp_id)
        if state is None or state.expert_name.strip() != expert_name:
            raise ContractError("EXPERT_NOT_FOUND")
        if (
            state.current_stage != "discovery_call"
            or state.stage_status != "intervention_required"
            or state.next_action != ADMIN_REVIEW_NEXT_ACTION
        ):
            raise StateTransitionError("ADMIN_REVIEW_NOT_REQUESTED")
        artifact_input = {
            "artifact_type": "esm_work_note",
            "version_role": "human_supplement",
            "stage": "discovery_call",
            "display_title": _required_text(command.get("display_title"), "display_title"),
            "controlled_access_url": _required_text(command.get("controlled_access_url"), "controlled_access_url"),
            "readable_content_descriptor": _required_text(
                command.get("readable_content_descriptor"),
                "readable_content_descriptor",
                maximum=280,
            ),
            "access_policy": "task_scoped",
        }
        artifact = await self._normalize_artifact(identity, state, artifact_input, admin_review=True)
        return (
            {
                "command_type": "admin_review",
                "skill": "expert-initial-outreach",
                "subject": temp_id,
                "temp_application_id": temp_id,
                "expert_name": expert_name,
                "artifact": asdict(artifact),
            },
            [],
        )

    async def _normalize_artifact(
        self,
        identity: Identity,
        state: Any,
        raw: Any,
        *,
        admin_review: bool = False,
    ) -> Artifact:
        if not isinstance(raw, dict):
            raise ContractError("ARTIFACT_CONTRACT_INVALID")
        _reject_unknown_fields(
            raw,
            {
                "artifact_type",
                "version_role",
                "stage",
                "display_title",
                "controlled_access_url",
                "readable_content_descriptor",
                "logical_artifact_id",
                "source_execution_id",
                "source_result_hash",
                "mime_type",
                "size_bytes",
                "language",
                "access_policy",
                "allowed_purposes",
            },
        )
        artifact_type = _required_text(raw.get("artifact_type"), "artifact_type")
        version_role = _required_text(raw.get("version_role"), "version_role")
        stage = _required_text(raw.get("stage"), "stage")
        if stage != state.current_stage:
            raise ContractError("ARTIFACT_STAGE_MISMATCH")
        display_title = _required_text(raw.get("display_title"), "display_title")
        url = _required_text(raw.get("controlled_access_url"), "controlled_access_url")
        descriptor = _optional_text(raw.get("readable_content_descriptor"))
        if descriptor is not None and len(descriptor) > 280:
            raise ContractError("ARTIFACT_COMMENT_TOO_LONG")
        logical_id = _optional_text(raw.get("logical_artifact_id")) or new_id("LART")
        existing = await self.storage.find_artifacts(
            temp_application_id=state.temp_application_id,
            artifact_type=artifact_type,
        )
        family = [item for item in existing if item.logical_artifact_id == logical_id]
        version = max((item.artifact_version for item in family), default=0) + 1
        supersedes = max(family, key=lambda item: item.artifact_version).artifact_id if family else None
        reviewed_at = (
            utc_minute_text() if version_role == "reviewed_final" else None
        )
        reviewed_by = identity.esm_owner_id if reviewed_at else None
        source_execution_id = _optional_text(raw.get("source_execution_id"))
        source_result_hash = _optional_text(raw.get("source_result_hash"))
        if "size_bytes" in raw and (
            isinstance(raw["size_bytes"], bool)
            or not isinstance(raw["size_bytes"], int)
            or raw["size_bytes"] < 0
        ):
            raise ContractError("ARTIFACT_CONTRACT_INVALID")
        if version_role == "reviewed_final" and source_execution_id is None:
            source_execution_id = new_id("EXEC")
        if admin_review and source_execution_id is None:
            source_execution_id = new_id("EXEC")
        kind = "work_product"
        if version_role == "source":
            kind = "external_reference" if artifact_type.endswith("_link") or artifact_type == "public_profile_reference" else "reference_source"
        artifact_id = new_id("ART")
        descriptor_hash = sha256_value(
            {
                "controlled_access_url": url,
                "display_title": display_title,
                "readable_content_descriptor": descriptor,
                "logical_artifact_id": logical_id,
                "artifact_version": version,
            }
        )
        return Artifact(
            artifact_id=artifact_id,
            logical_artifact_id=logical_id,
            artifact_version=version,
            artifact_schema_version="1.1.0",
            artifact_type=artifact_type,
            artifact_kind=kind,
            version_role=version_role,
            artifact_status="active",
            source_execution_id=source_execution_id,
            source_result_hash=source_result_hash,
            application_id=None,
            temp_application_id=state.temp_application_id,
            expert_name_snapshot=state.expert_name,
            esm_owner_id=state.esm_owner_id,
            esm_owner_display_name=state.esm_owner_display_name,
            stage=stage,
            display_title=display_title,
            storage_type="controlled_external_url",
            content_page_or_object_id=None,
            controlled_access_url=url,
            mime_type=_optional_text(raw.get("mime_type")),
            size_bytes=raw.get("size_bytes"),
            language=_string_list(raw.get("language", [])),
            readable_content_descriptor=descriptor,
            content_hash=descriptor_hash,
            created_at=utc_text(),
            created_by_actor_type="esm",
            created_by_actor_id=identity.esm_owner_id,
            reviewed_at=reviewed_at,
            reviewed_by_actor_id=reviewed_by,
            supersedes_artifact_id=supersedes,
            access_policy="task_scoped" if admin_review else raw.get("access_policy", "owner_only"),
            allowed_purposes=_string_list(raw.get("allowed_purposes", [])),
        )

    async def _preview_transition(
        self,
        state: Any,
        identity: Identity,
        event_type: str,
        payload: dict[str, Any],
        artifact: Artifact | None,
        reference_artifact_ids: list[str],
        target_event_id: str | None,
    ) -> None:
        if event_type == "positioning_progress_assessed":
            stream = await self.storage.stream_events(state.temp_application_id)
            if not stream or stream[-1].stream_version != state.last_processed_stream_version:
                raise StateTransitionError("PROJECTION_NOT_CURRENT")
            authoritative = replay_stream(stream)
            if (authoritative.esm_owner_id != identity.esm_owner_id
                or authoritative.current_stage != "agent_positioning"
                or authoritative.stage_status != "awaiting_progress_assessment"):
                raise StateTransitionError("INVALID_STATE_TRANSITION")
        event = Event(
            event_id="EVT-PREVIEW",
            global_sequence=state.last_processed_global_sequence + 1,
            event_type=event_type,
            stream_version=state.last_processed_stream_version + 1,
            temp_application_id=state.temp_application_id,
            occurred_at=utc_text(),
            recorded_at=utc_text(),
            actor_type="esm",
            actor_id=identity.esm_owner_id,
            esm_owner_id_at_event_time=state.esm_owner_id,
            esm_owner_display_name_at_event_time=state.esm_owner_display_name,
            source_session_id="preview",
            execution_id=None,
            correlation_id="COR-PREVIEW",
            causation_id=None,
            idempotency_key="IDEMP-PREVIEW",
            command_hash=sha256_value({"event_type": event_type, "payload": payload}),
            schema_version="1.1.0",
            payload=payload,
            payload_hash=sha256_value(payload),
            artifact_reference_ids=(
                ([artifact.artifact_id] if artifact else []) + reference_artifact_ids
            ),
            target_event_id=target_event_id,
        )
        if event_type == "event_corrected":
            replay_stream(
                (await self.storage.stream_events(state.temp_application_id)) + [event]
            )
        else:
            reduce_event(deepcopy(state), event)

    async def _confirm_batch(
        self, identity: Identity, source_session_id: str, claims: dict[str, Any]
    ) -> dict[str, Any]:
        command = claims["command"]
        results: list[dict[str, Any]] = []
        pending_projection_checks: list[tuple[int, str, str]] = []
        for candidate in command["candidates"]:
            temp_id = candidate["temp_application_id"]
            try:
                created_payload = _application_created_payload(
                    expert_name=candidate["expert_name"],
                    source=candidate["source"],
                    professional_domains=candidate["professional_domains"],
                    market_regions=candidate["market_regions"],
                    entry_path=candidate["entry_path"],
                    trusted_entry_reason_short=candidate[
                        "trusted_entry_reason_short"
                    ],
                )
                existing_events = await self.storage.stream_events(temp_id)
                if existing_events:
                    event = existing_events[0]
                else:
                    event = await self._append(
                        identity=identity,
                        owner_id=candidate["owner_id"],
                        owner_display_name=candidate["owner_display_name"],
                        source_session_id=source_session_id,
                        confirmation_id=str(claims["confirmation_id"]),
                        temp_application_id=temp_id,
                        event_type="application_created",
                        payload=created_payload,
                        artifact_ids=[],
                        suffix="create",
                        idempotency_scope=command["batch_id"],
                    )
                last_event = event
                if candidate.get("primary_public_profile_url"):
                    existing_profile = next(
                        (
                            item
                            for item in existing_events
                            if item.event_type == "expert_profile_updated"
                            and item.payload.get("primary_public_profile_url")
                            == candidate["primary_public_profile_url"]
                        ),
                        None,
                    )
                    if existing_profile is not None:
                        last_event = existing_profile
                    else:
                        last_event = await self._append(
                            identity=identity,
                            owner_id=candidate["owner_id"],
                            owner_display_name=candidate["owner_display_name"],
                            source_session_id=source_session_id,
                            confirmation_id=str(claims["confirmation_id"]),
                            temp_application_id=temp_id,
                            event_type="expert_profile_updated",
                            payload={
                                "reason": "initial_public_profile_registration",
                                "primary_public_profile_url": candidate[
                                    "primary_public_profile_url"
                                ],
                            },
                            artifact_ids=[],
                            suffix="profile",
                            idempotency_scope=command["batch_id"],
                        )
                self.projector.notify()
                results.append(
                    {
                        "expert_name": candidate["expert_name"],
                        "temp_application_id": temp_id,
                        "status": "projection_pending",
                        "result_code": "WRITE_COMMITTED_PROJECTION_PENDING",
                        "event_id": last_event.event_id,
                        "artifact_commit_acknowledged": None,
                        "artifact_readback_succeeded": None,
                        "event_commit_acknowledged": True,
                        "projection_readback_succeeded": False,
                        "pending_phases": ["projection_readback"],
                    }
                )
                pending_projection_checks.append(
                    (len(results) - 1, temp_id, last_event.event_id)
                )
            except GatewayError as exc:
                results.append(
                    {
                        "expert_name": candidate["expert_name"],
                        "temp_application_id": temp_id,
                        "status": "failed",
                        "result_code": exc.result_code,
                        "failed_phase": (
                            exc.failed_phase
                            if isinstance(exc, PersistenceError)
                            else None
                        ),
                        "artifact_commit_acknowledged": (
                            exc.artifact_commit_acknowledged
                            if isinstance(exc, PersistenceError)
                            else None
                        ),
                        "artifact_readback_succeeded": (
                            exc.artifact_readback_succeeded
                            if isinstance(exc, PersistenceError)
                            else None
                        ),
                        "event_commit_acknowledged": (
                            exc.event_commit_acknowledged
                            if isinstance(exc, PersistenceError)
                            else None
                        ),
                        "projection_readback_succeeded": None,
                        "pending_phases": [],
                    }
                )
        if pending_projection_checks:
            projected_items = await asyncio.gather(
                *(
                    self._wait_for_projection(temp_id, event_id)
                    for _index, temp_id, event_id in pending_projection_checks
                )
            )
            for (index, _temp_id, _event_id), projected in zip(
                pending_projection_checks, projected_items, strict=True
            ):
                if projected:
                    results[index]["status"] = "success"
                    results[index]["result_code"] = "WRITE_COMPLETED"
                    results[index]["projection_readback_succeeded"] = True
                    results[index]["pending_phases"] = []
        succeeded = sum(item["status"] in {"success", "projection_pending"} for item in results)
        has_pending = any(item["status"] == "projection_pending" for item in results)
        if succeeded != len(results):
            overall_status = "partial_failure"
            overall_code = "BATCH_WRITE_PARTIAL_FAILURE"
        elif has_pending:
            overall_status = "pending"
            overall_code = "BATCH_WRITE_COMMITTED_PROJECTION_PENDING"
        else:
            overall_status = "success"
            overall_code = "BATCH_WRITE_COMPLETED"
        return {
            "status": overall_status,
            "result_code": overall_code,
            "identity_verified": True,
            "batch_id": command["batch_id"],
            "results": results,
            "credential_material_returned": False,
        }

    async def _confirm_business_event(
        self, identity: Identity, source_session_id: str, claims: dict[str, Any]
    ) -> dict[str, Any]:
        command = claims["command"]
        state = await self.storage.get_state(command["temp_application_id"])
        if state is None or state.esm_owner_id != identity.esm_owner_id:
            raise AuthorizationError("EXPERT_WRITE_FORBIDDEN_OWNER_MISMATCH")
        if state.last_processed_stream_version != command["expected_stream_version"]:
            raise StateTransitionError("STREAM_VERSION_CONFLICT")
        if command["event_type"] == "positioning_progress_assessed":
            await self._preview_transition(state, identity, command["event_type"], command["payload"], None, [], None)
        artifact = Artifact(**command["artifact"]) if command.get("artifact") else None
        artifact_receipt = None
        if artifact is not None:
            try:
                artifact_receipt = await self.storage.put_artifact(artifact)
            except PersistenceError as exc:
                acknowledged = exc.artifact_commit_acknowledged
                if acknowledged is None and not exc.write_may_have_succeeded:
                    acknowledged = False
                raise exc.with_context(
                    failed_phase=exc.failed_phase or "artifact_write",
                    artifact_id=artifact.artifact_id,
                    artifact_commit_acknowledged=acknowledged,
                    event_commit_acknowledged=False,
                ) from exc
        artifact_ids = [artifact.artifact_id] if artifact else []
        artifact_ids.extend(command.get("reference_artifact_ids", []))
        try:
            event = await self._append(
                identity=identity,
                owner_id=state.esm_owner_id,
                owner_display_name=state.esm_owner_display_name,
                source_session_id=source_session_id,
                confirmation_id=str(claims["confirmation_id"]),
                temp_application_id=state.temp_application_id,
                event_type=command["event_type"],
                payload=command["payload"],
                artifact_ids=artifact_ids,
                suffix="business",
                target_event_id=command.get("target_event_id"),
            )
        except PersistenceError as exc:
            event_acknowledged = exc.event_commit_acknowledged
            if event_acknowledged is None and not exc.write_may_have_succeeded:
                event_acknowledged = False
            raise exc.with_context(
                failed_phase=exc.failed_phase or "event_append",
                artifact_id=artifact.artifact_id if artifact else None,
                artifact_commit_acknowledged=(
                    artifact_receipt.commit_acknowledged
                    if artifact_receipt is not None
                    else None
                ),
                artifact_readback_succeeded=(
                    artifact_receipt.readback_succeeded
                    if artifact_receipt is not None
                    else None
                ),
                event_commit_acknowledged=event_acknowledged,
            ) from exc
        self.projector.notify()
        projected, artifact_visible = await asyncio.gather(
            self._wait_for_projection(state.temp_application_id, event.event_id),
            self._wait_for_artifact_readback(artifact),
        )
        if projected and artifact_visible:
            result_code = "WRITE_COMPLETED"
        elif not artifact_visible:
            result_code = "WRITE_COMMITTED_READBACK_PENDING"
        else:
            result_code = "WRITE_COMMITTED_PROJECTION_PENDING"
        pending_phases = []
        if not artifact_visible:
            pending_phases.append("artifact_readback")
        if not projected:
            pending_phases.append("projection_readback")
        return {
            "status": "success" if result_code == "WRITE_COMPLETED" else "pending",
            "result_code": result_code,
            "identity_verified": True,
            "temp_application_id": state.temp_application_id,
            "event_id": event.event_id,
            "artifact_id": artifact.artifact_id if artifact else None,
            "artifact_commit_acknowledged": (
                artifact_receipt.commit_acknowledged
                if artifact_receipt is not None
                else None
            ),
            "artifact_readback_succeeded": artifact_visible if artifact else None,
            "event_commit_acknowledged": True,
            "projection_readback_succeeded": projected,
            "pending_phases": pending_phases,
            "credential_material_returned": False,
        }

    async def _confirm_admin_review(
        self, identity: Identity, claims: dict[str, Any]
    ) -> dict[str, Any]:
        if not identity.is_admin:
            raise AuthorizationError("ADMIN_ROLE_REQUIRED")
        command = claims["command"]
        state = await self.storage.get_state(command["temp_application_id"])
        if (
            state is None
            or state.expert_name != command["expert_name"]
            or state.current_stage != "discovery_call"
            or state.stage_status != "intervention_required"
            or state.next_action != ADMIN_REVIEW_NEXT_ACTION
        ):
            raise StateTransitionError("ADMIN_REVIEW_NOT_REQUESTED")
        artifact = Artifact(**command["artifact"])
        receipt = await self.storage.put_artifact(artifact)
        readback_succeeded = await self._wait_for_artifact_readback(artifact)
        return {
            "status": "success" if readback_succeeded else "pending",
            "result_code": (
                "ADMIN_REVIEW_ARTIFACT_SAVED"
                if readback_succeeded
                else "ADMIN_REVIEW_ARTIFACT_COMMITTED_READBACK_PENDING"
            ),
            "identity_verified": True,
            "artifact_id": receipt.artifact.artifact_id,
            "artifact_created": receipt.created,
            "artifact_commit_acknowledged": receipt.commit_acknowledged,
            "artifact_readback_succeeded": readback_succeeded,
            "event_commit_acknowledged": None,
            "state_changed": False,
            "credential_material_returned": False,
        }

    async def _append(
        self,
        *,
        identity: Identity,
        owner_id: str,
        owner_display_name: str,
        source_session_id: str,
        confirmation_id: str,
        temp_application_id: str,
        event_type: str,
        payload: dict[str, Any],
        artifact_ids: list[str],
        suffix: str,
        target_event_id: str | None = None,
        idempotency_scope: str | None = None,
    ) -> Event:
        command_hash = sha256_value(
            {
                "event_type": event_type,
                "temp_application_id": temp_application_id,
                "payload": payload,
                "artifact_ids": artifact_ids,
            }
        )
        draft = EventDraft(
            event_type=event_type,
            temp_application_id=temp_application_id,
            actor_type="esm",
            actor_id=identity.esm_owner_id,
            esm_owner_id=owner_id,
            esm_owner_display_name=owner_display_name,
            source_session_id=source_session_id,
            execution_id=None,
            correlation_id=confirmation_id,
            causation_id=confirmation_id,
            idempotency_key=(
                f"{idempotency_scope or confirmation_id}:{temp_application_id}:{suffix}"
            ),
            command_hash=command_hash,
            payload=payload,
            artifact_reference_ids=artifact_ids,
            occurred_at=utc_text(),
            target_event_id=target_event_id,
        )
        event, _created = await self.storage.append_event(draft)
        return event

    async def _wait_for_projection(self, temp_id: str, event_id: str) -> bool:
        try:
            state = await self.storage.get_state(temp_id)
        except GatewayError:
            state = None
        if state is not None and state.last_applied_event_id == event_id:
            return True
        if self.projection_wait_seconds <= 0:
            return False
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.projection_wait_seconds
        retry_delay_seconds = 0.05
        while loop.time() < deadline:
            # A notify can race ahead of Notion's cross-token visibility. Wake the
            # Worker again with bounded exponential backoff while also checking the
            # independently stored ExpertState projection.
            self.projector.notify()
            remaining_seconds = deadline - loop.time()
            await asyncio.sleep(min(retry_delay_seconds, remaining_seconds))
            try:
                state = await self.storage.get_state(temp_id)
            except GatewayError:
                state = None
            if state is not None and state.last_applied_event_id == event_id:
                return True
            retry_delay_seconds = min(retry_delay_seconds * 2, 5.0)
        return False

    async def _wait_for_artifact_readback(self, artifact: Artifact | None) -> bool:
        if artifact is None:
            return True

        async def visible() -> bool:
            try:
                stored = await self.storage.get_artifact(artifact.artifact_id)
                if stored is None or not artifacts_equivalent(artifact, stored):
                    return False
                if artifact.supersedes_artifact_id:
                    predecessor = await self.storage.get_artifact(
                        artifact.supersedes_artifact_id
                    )
                    return (
                        predecessor is not None
                        and predecessor.artifact_status == "superseded"
                    )
                return True
            except GatewayError:
                return False

        if await visible():
            return True
        if self.projection_wait_seconds <= 0:
            return False
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.projection_wait_seconds
        retry_delay_seconds = 0.05
        while loop.time() < deadline:
            remaining_seconds = deadline - loop.time()
            await asyncio.sleep(min(retry_delay_seconds, remaining_seconds))
            if await visible():
                return True
            retry_delay_seconds = min(retry_delay_seconds * 2, 5.0)
        return False

    def _require_identity(self, raw_code: str) -> Identity:
        identity = self.identities.verify(raw_code)
        if identity is None:
            raise AuthorizationError("IDENTITY_DENIED")
        return identity

    @staticmethod
    def _success(code: str, identity: Identity, **values: Any) -> dict[str, Any]:
        return {
            "status": "success",
            "result_code": code,
            "identity_verified": True,
            **values,
            "credential_material_returned": False,
        }

    @staticmethod
    def _failure(code: str, *, identity_verified: bool) -> dict[str, Any]:
        return {
            "status": "failed",
            "result_code": code,
            "identity_verified": identity_verified,
            "credential_material_returned": False,
        }


def _bind_artifact_payload(
    event_type: str, payload: dict[str, Any], artifact: Artifact
) -> dict[str, Any]:
    result = deepcopy(payload)
    if event_type == "stage_ai_draft_generated":
        result.update(
            {
                "artifact_type": artifact.artifact_type,
                "artifact_id": artifact.artifact_id,
                "artifact_version": artifact.artifact_version,
            }
        )
    elif event_type == "stage_review_requested":
        result["artifact_id"] = artifact.artifact_id
    elif event_type == "stage_reviewed_final_confirmed":
        result.update(
            {
                "artifact_type": artifact.artifact_type,
                "artifact_id": artifact.artifact_id,
                "artifact_version": artifact.artifact_version,
                "reviewed_at": artifact.reviewed_at,
            }
        )
    elif event_type in {"discovery_source_registered", "positioning_source_registered"}:
        result["artifact_id"] = artifact.artifact_id
        result["source_type"] = (
            "transcript" if artifact.artifact_type.endswith("_transcript") else "notes"
        )
    elif event_type == "outreach_sent":
        result["artifact_id"] = artifact.artifact_id
    elif event_type == "outreach_response_recorded":
        result["artifact_id"] = artifact.artifact_id
    elif event_type == "call_prep_review_completed":
        result["artifact_id"] = artifact.artifact_id
    return result


def _public_preview(command: dict[str, Any]) -> dict[str, Any]:
    preview = deepcopy(command)
    if command["command_type"] in {"import_experts_csv_batch", "retry_csv_import_batch"}:
        preview.pop("csv_text", None)
        preview.pop("all_rows", None)
        preview.pop("expected_revision", None)
        preview.pop("import_actor_id", None)
        for candidate in preview["candidates"]:
            candidate.pop("payload", None)
            for asset in candidate["artifacts"]:
                asset.pop("content_hash", None)
    preview.pop("terminal_warning", None)
    artifact = preview.get("artifact")
    if isinstance(artifact, dict):
        artifact.pop("content_hash", None)
    if preview.get("event_type") == "event_corrected" and isinstance(
        preview.get("payload"), dict
    ):
        preview["payload"].pop("target_event_hash", None)
        preview["payload"].pop("confirmation_id", None)
    return preview


def _required_text(value: Any, field_name: str, *, maximum: int = 500) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > maximum:
        raise ContractError("COMMAND_PAYLOAD_INVALID", f"Invalid {field_name}.")
    return value.strip()


def _optional_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ContractError("COMMAND_PAYLOAD_INVALID")
    return value.strip() or None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ContractError("COMMAND_PAYLOAD_INVALID")
    return [item for item in value if item]


def _frozen_enum_list(
    value: Any, *, allowed: frozenset[str], field_name: str
) -> list[str]:
    items = _string_list(value)
    if not set(items).issubset(allowed):
        raise ContractError("COMMAND_PAYLOAD_INVALID", f"Invalid {field_name}.")
    return items


def _application_created_payload(
    *,
    expert_name: str,
    source: str,
    professional_domains: list[str],
    market_regions: list[str],
    entry_path: str,
    trusted_entry_reason_short: str | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "expert_name": expert_name,
        "source": source,
        "professional_domains": professional_domains,
        "market_regions": market_regions,
        "entry_path": entry_path,
    }
    if entry_path == "trusted_direct":
        payload["skipped_stages"] = ["sourcing", "evaluation"]
        payload["trusted_entry_reason_short"] = trusted_entry_reason_short
    return payload


def _reject_unknown_fields(value: dict[str, Any], allowed: set[str]) -> None:
    if set(value) - allowed:
        raise ContractError("COMMAND_UNKNOWN_FIELDS")


def _is_temp_application_id(value: str) -> bool:
    return _is_prefixed_uuid(value, "TMP")


def _is_prefixed_uuid(value: str, prefix: str) -> bool:
    import uuid

    marker = f"{prefix}-"
    if not value.startswith(marker):
        return False
    try:
        uuid.UUID(value[len(marker):])
    except ValueError:
        return False
    return True
