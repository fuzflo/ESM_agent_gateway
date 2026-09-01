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
    ) -> None:
        super().__init__(result_code, message)
        self.write_may_have_succeeded = write_may_have_succeeded

