"""Stable, sanitized errors returned by the V1 Gateway."""

from __future__ import annotations


class GatewayError(RuntimeError):
    """A safe error with a stable machine-readable result code."""

    def __init__(self, result_code: str, message: str = "") -> None:
        super().__init__(message or result_code)
        self.result_code = result_code
        self.safe_message = message or result_code


class ConfigurationError(GatewayError):
    pass


class ContractError(GatewayError):
    pass


class AuthorizationError(GatewayError):
    pass


class StateTransitionError(GatewayError):
    pass


class PersistenceError(GatewayError):
    """Provider failure without raw provider bodies or credentials."""

    def __init__(
        self,
        result_code: str,
        message: str = "",
        *,
        write_may_have_succeeded: bool = False,
        failed_phase: str | None = None,
        artifact_id: str | None = None,
        event_id: str | None = None,
        artifact_commit_acknowledged: bool | None = None,
        artifact_readback_succeeded: bool | None = None,
        event_commit_acknowledged: bool | None = None,
    ) -> None:
        super().__init__(result_code, message)
        self.write_may_have_succeeded = write_may_have_succeeded
        self.failed_phase = failed_phase
        self.artifact_id = artifact_id
        self.event_id = event_id
        self.artifact_commit_acknowledged = artifact_commit_acknowledged
        self.artifact_readback_succeeded = artifact_readback_succeeded
        self.event_commit_acknowledged = event_commit_acknowledged

    def with_context(self, **values: object) -> "PersistenceError":
        return PersistenceError(
            self.result_code,
            self.safe_message,
            write_may_have_succeeded=self.write_may_have_succeeded,
            failed_phase=values.get("failed_phase", self.failed_phase),
            artifact_id=values.get("artifact_id", self.artifact_id),
            event_id=values.get("event_id", self.event_id),
            artifact_commit_acknowledged=values.get(
                "artifact_commit_acknowledged",
                self.artifact_commit_acknowledged,
            ),
            artifact_readback_succeeded=values.get(
                "artifact_readback_succeeded",
                self.artifact_readback_succeeded,
            ),
            event_commit_acknowledged=values.get(
                "event_commit_acknowledged",
                self.event_commit_acknowledged,
            ),
        )
