"""Storage protocols plus an exact in-memory implementation for tests."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Protocol

from .contracts import Artifact, Event, ExpertState
from .errors import ContractError
from .util import new_id, sha256_value, utc_text


@dataclass(frozen=True, slots=True)
class EventDraft:
    event_type: str
    temp_application_id: str
    actor_type: str
    actor_id: str
    esm_owner_id: str
    esm_owner_display_name: str
    source_session_id: str
    execution_id: str | None
    correlation_id: str
    causation_id: str | None
    idempotency_key: str
    command_hash: str
    payload: dict[str, Any]
    artifact_reference_ids: list[str]
    occurred_at: str
    target_event_id: str | None = None


@dataclass(frozen=True, slots=True)
class ArtifactWriteReceipt:
    artifact: Artifact
    created: bool
    commit_acknowledged: bool
    readback_succeeded: bool


_ARTIFACT_UNORDERED_LIST_FIELDS = frozenset({"language", "allowed_purposes"})
_ARTIFACT_PROVIDER_NORMALIZED_FIELDS = frozenset({"created_at"})
_ARTIFACT_TIMESTAMP_FIELDS = frozenset(
    {"reviewed_at", "link_last_checked_at", "retention_until"}
)


def _normalized_artifact_value(field_name: str, value: Any) -> Any:
    if field_name in _ARTIFACT_UNORDERED_LIST_FIELDS and isinstance(value, list):
        return tuple(sorted(set(value)))
    if field_name not in _ARTIFACT_TIMESTAMP_FIELDS or not isinstance(value, str):
        return value
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        instant = datetime.fromisoformat(candidate)
    except ValueError:
        return value
    if instant.tzinfo is None:
        return instant
    return instant.astimezone(timezone.utc)


def _valid_iso_date_or_datetime(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        datetime.fromisoformat(candidate)
    except ValueError:
        return False
    return True


def artifact_mismatch_fields(expected: Artifact, actual: Artifact) -> tuple[str, ...]:
    """Return only field names whose provider-round-tripped values differ."""

    expected_record = expected.as_record()
    actual_record = actual.as_record()
    mismatches: list[str] = []
    for field_name in expected_record:
        expected_value = expected_record[field_name]
        actual_value = actual_record[field_name]
        if field_name in _ARTIFACT_PROVIDER_NORMALIZED_FIELDS:
            if not (
                _valid_iso_date_or_datetime(expected_value)
                and _valid_iso_date_or_datetime(actual_value)
            ):
                mismatches.append(field_name)
            continue
        if _normalized_artifact_value(
            field_name, expected_value
        ) != _normalized_artifact_value(field_name, actual_value):
            mismatches.append(field_name)
    return tuple(mismatches)


def artifacts_equivalent(expected: Artifact, actual: Artifact) -> bool:
    return not artifact_mismatch_fields(expected, actual)


class Storage(Protocol):
    async def append_event(self, draft: EventDraft) -> tuple[Event, bool]: ...

    async def find_event_by_idempotency(
        self, temp_application_id: str, idempotency_key: str
    ) -> Event | None: ...

    async def events_after(self, global_sequence: int, limit: int = 100) -> list[Event]: ...

    async def stream_events(self, temp_application_id: str) -> list[Event]: ...

    async def get_state(self, temp_application_id: str) -> ExpertState | None: ...

    async def find_states_by_exact_name(self, expert_name: str) -> list[ExpertState]: ...

    async def find_state_by_public_profile_url(self, url: str) -> ExpertState | None: ...

    async def public_profile_url_exists(self, url: str) -> bool: ...

    async def upsert_state(self, state: ExpertState) -> None: ...

    async def put_artifact(self, artifact: Artifact) -> ArtifactWriteReceipt: ...

    async def get_artifact(self, artifact_id: str) -> Artifact | None: ...

    async def mark_artifact_orphaned(
        self, artifact_id: str, temp_application_id: str
    ) -> Artifact: ...

    async def find_artifacts(
        self,
        *,
        temp_application_id: str,
        artifact_type: str | None = None,
        version_role: str | None = None,
    ) -> list[Artifact]: ...

    async def get_checkpoint(self) -> int: ...

    async def set_checkpoint(self, global_sequence: int, event_id: str) -> None: ...


class InMemoryStorage:
    """Atomic enough to test all deterministic V1 behavior without provider calls."""

    def __init__(self) -> None:
        self.events: list[Event] = []
        self.states: dict[str, ExpertState] = {}
        self.artifacts: dict[str, Artifact] = {}
        self.checkpoint = 0
        self.checkpoint_event_id: str | None = None
        self._lock = asyncio.Lock()

    async def append_event(self, draft: EventDraft) -> tuple[Event, bool]:
        async with self._lock:
            existing = next(
                (
                    event
                    for event in self.events
                    if event.temp_application_id == draft.temp_application_id
                    and event.idempotency_key == draft.idempotency_key
                ),
                None,
            )
            if existing is not None:
                if existing.command_hash != draft.command_hash:
                    raise ContractError("IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_COMMAND")
                return deepcopy(existing), False
            stream_version = (
                max(
                    (
                        event.stream_version
                        for event in self.events
                        if event.temp_application_id == draft.temp_application_id
                    ),
                    default=0,
                )
                + 1
            )
            global_sequence = len(self.events) + 1
            event = Event(
                event_id=new_id("EVT"),
                global_sequence=global_sequence,
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
                schema_version="1.1.0",
                payload=deepcopy(draft.payload),
                payload_hash=sha256_value(draft.payload),
                artifact_reference_ids=list(draft.artifact_reference_ids),
                target_event_id=draft.target_event_id,
            )
            self.events.append(event)
            return deepcopy(event), True

    async def find_event_by_idempotency(
        self, temp_application_id: str, idempotency_key: str
    ) -> Event | None:
        return next(
            (
                deepcopy(event)
                for event in self.events
                if event.temp_application_id == temp_application_id
                and event.idempotency_key == idempotency_key
            ),
            None,
        )

    async def events_after(self, global_sequence: int, limit: int = 100) -> list[Event]:
        return [
            deepcopy(event)
            for event in self.events
            if event.global_sequence > global_sequence
        ][:limit]

    async def stream_events(self, temp_application_id: str) -> list[Event]:
        return [
            deepcopy(event)
            for event in self.events
            if event.temp_application_id == temp_application_id
        ]

    async def get_state(self, temp_application_id: str) -> ExpertState | None:
        state = self.states.get(temp_application_id)
        return deepcopy(state) if state is not None else None

    async def find_states_by_exact_name(self, expert_name: str) -> list[ExpertState]:
        return [
            deepcopy(state)
            for state in self.states.values()
            if state.expert_name.strip() == expert_name.strip()
        ]

    async def find_state_by_public_profile_url(self, url: str) -> ExpertState | None:
        return next(
            (
                deepcopy(state)
                for state in self.states.values()
                if state.values.get("primary_public_profile_url") == url
            ),
            None,
        )

    async def public_profile_url_exists(self, url: str) -> bool:
        if await self.find_state_by_public_profile_url(url) is not None:
            return True
        return any(
            event.payload.get("primary_public_profile_url") == url
            for event in self.events
            if event.event_type
            in {"expert_profile_updated", "sourcing_assessment_recorded"}
        )

    async def upsert_state(self, state: ExpertState) -> None:
        self.states[state.temp_application_id] = deepcopy(state)

    async def put_artifact(self, artifact: Artifact) -> ArtifactWriteReceipt:
        async with self._lock:
            existing = self.artifacts.get(artifact.artifact_id)
            if existing is not None:
                if existing != artifact:
                    raise ContractError("ARTIFACT_ID_REUSED_WITH_DIFFERENT_CONTENT")
                self._supersede_predecessor(artifact)
                return ArtifactWriteReceipt(
                    artifact=deepcopy(existing),
                    created=False,
                    commit_acknowledged=True,
                    readback_succeeded=True,
                )
            for candidate in self.artifacts.values():
                if (
                    candidate.logical_artifact_id == artifact.logical_artifact_id
                    and candidate.artifact_version == artifact.artifact_version
                ):
                    raise ContractError("ARTIFACT_VERSION_CONFLICT")
            self.artifacts[artifact.artifact_id] = deepcopy(artifact)
            self._supersede_predecessor(artifact)
            return ArtifactWriteReceipt(
                artifact=deepcopy(artifact),
                created=True,
                commit_acknowledged=True,
                readback_succeeded=True,
            )

    def _supersede_predecessor(self, artifact: Artifact) -> None:
        if not artifact.supersedes_artifact_id:
            return
        predecessor = self.artifacts.get(artifact.supersedes_artifact_id)
        if (
            predecessor is None
            or predecessor.logical_artifact_id != artifact.logical_artifact_id
            or predecessor.artifact_version >= artifact.artifact_version
        ):
            raise ContractError("ARTIFACT_SUPERSEDES_INVALID")
        if predecessor.artifact_status == "active":
            self.artifacts[predecessor.artifact_id] = replace(
                predecessor, artifact_status="superseded"
            )

    async def get_artifact(self, artifact_id: str) -> Artifact | None:
        artifact = self.artifacts.get(artifact_id)
        return deepcopy(artifact) if artifact is not None else None

    async def mark_artifact_orphaned(
        self, artifact_id: str, temp_application_id: str
    ) -> Artifact:
        async with self._lock:
            artifact = self.artifacts.get(artifact_id)
            if artifact is None or artifact.temp_application_id != temp_application_id:
                raise ContractError("ARTIFACT_NOT_FOUND")
            if artifact.artifact_status != "active":
                raise ContractError("ARTIFACT_NOT_ACTIVE")
            updated = replace(artifact, artifact_status="orphaned")
            self.artifacts[artifact_id] = updated
            return deepcopy(updated)

    async def find_artifacts(
        self,
        *,
        temp_application_id: str,
        artifact_type: str | None = None,
        version_role: str | None = None,
    ) -> list[Artifact]:
        matches = [
            artifact
            for artifact in self.artifacts.values()
            if artifact.temp_application_id == temp_application_id
            and (artifact_type is None or artifact.artifact_type == artifact_type)
            and (version_role is None or artifact.version_role == version_role)
        ]
        return sorted(
            (deepcopy(item) for item in matches),
            key=lambda item: (item.logical_artifact_id, item.artifact_version),
        )

    async def get_checkpoint(self) -> int:
        return self.checkpoint

    async def set_checkpoint(self, global_sequence: int, event_id: str) -> None:
        if global_sequence < self.checkpoint:
            raise ContractError("CHECKPOINT_REGRESSION")
        self.checkpoint = global_sequence
        self.checkpoint_event_id = event_id
