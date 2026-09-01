"""Server-controlled ten-person ESM identity mapping."""

from __future__ import annotations

import hmac
import json
from dataclasses import dataclass
from typing import Any

from .errors import ConfigurationError


@dataclass(frozen=True, slots=True)
class Identity:
    esm_owner_id: str
    display_name: str
    role: str

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


class IdentityDirectory:
    """Resolve raw codes without ever returning or logging credential material."""

    def __init__(self, by_code: dict[str, Identity]) -> None:
        if len(by_code) != 10:
            raise ConfigurationError(
                "IDENTITY_MAP_INVALID", "Exactly ten active identities are required."
            )
        identities = list(by_code.values())
        if sum(identity.role == "admin" for identity in identities) != 1:
            raise ConfigurationError(
                "IDENTITY_MAP_INVALID", "Exactly one ESM Admin is required."
            )
        if sum(identity.role == "esm" for identity in identities) != 9:
            raise ConfigurationError(
                "IDENTITY_MAP_INVALID", "Exactly nine ordinary ESMs are required."
            )
        ids = [identity.esm_owner_id for identity in identities]
        if len(set(ids)) != len(ids):
            raise ConfigurationError(
                "IDENTITY_MAP_INVALID", "esm_owner_id values must be unique."
            )
        self._by_code = dict(by_code)
        self._by_id = {identity.esm_owner_id: identity for identity in identities}

    @classmethod
    def from_json(cls, raw: str) -> "IdentityDirectory":
        try:
            data = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ConfigurationError("IDENTITY_MAP_INVALID") from exc
        if not isinstance(data, dict):
            raise ConfigurationError("IDENTITY_MAP_INVALID")
        parsed: dict[str, Identity] = {}
        for code, record in data.items():
            if not isinstance(code, str) or not code or not isinstance(record, dict):
                raise ConfigurationError("IDENTITY_MAP_INVALID")
            owner_id = record.get("esm_owner_id")
            display_name = record.get("display_name")
            role = record.get("role")
            if (
                not isinstance(owner_id, str)
                or not owner_id
                or not isinstance(display_name, str)
                or not display_name
                or role not in {"admin", "esm"}
                or set(record) != {"esm_owner_id", "display_name", "role"}
            ):
                raise ConfigurationError("IDENTITY_MAP_INVALID")
            parsed[code] = Identity(owner_id, display_name, role)
        return cls(parsed)

    def verify(self, raw_code: str) -> Identity | None:
        if not isinstance(raw_code, str) or not raw_code:
            return None
        matched: Identity | None = None
        for expected, identity in self._by_code.items():
            if hmac.compare_digest(raw_code, expected):
                matched = identity
        return matched

    def require_owner(self, owner_id: str) -> Identity:
        identity = self._by_id.get(owner_id)
        if identity is None:
            raise ConfigurationError("TARGET_OWNER_NOT_FOUND")
        return identity

    def get_by_id(self, owner_id: str) -> Identity | None:
        return self._by_id.get(owner_id)

    def public_roster(self) -> list[dict[str, Any]]:
        return [
            {
                "esm_owner_id": identity.esm_owner_id,
                "display_name": identity.display_name,
                "role": identity.role,
            }
            for identity in sorted(self._by_id.values(), key=lambda item: item.esm_owner_id)
        ]
