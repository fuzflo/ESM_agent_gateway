"""Ten-minute, single-use, payload-bound confirmation cards."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from .errors import ContractError
from .util import canonical_json, new_id, utc_now, utc_text


CONFIRMATION_PHRASE = "确定以上所有信息，请你写入数据库"
CONFIRMATION_TTL_SECONDS = 600


def _encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode(raw: str) -> bytes:
    return base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))


@dataclass(frozen=True, slots=True)
class ConfirmationCard:
    token: str
    confirmation_id: str
    expires_at: str


class ConfirmationService:
    def __init__(self, signing_key: str) -> None:
        if len(signing_key.encode("utf-8")) < 32:
            raise ContractError("CONFIRMATION_SIGNING_KEY_INVALID")
        self._key = signing_key.encode("utf-8")
        self._active_by_session: dict[str, str] = {}
        self._lock = threading.Lock()

    def issue(
        self,
        *,
        source_session_id: str,
        identity_id: str,
        skill: str,
        action: str,
        subject: str,
        command: dict[str, Any],
        confirmation_id: str | None = None,
    ) -> ConfirmationCard:
        if not source_session_id.strip():
            raise ContractError("SOURCE_SESSION_ID_REQUIRED")
        now = utc_now()
        expires = now + timedelta(seconds=CONFIRMATION_TTL_SECONDS)
        confirmation_id = confirmation_id or new_id("CONF")
        claims = {
            "confirmation_id": confirmation_id,
            "source_session_id": source_session_id,
            "identity_id": identity_id,
            "skill": skill,
            "action": action,
            "subject": subject,
            "command": command,
            "issued_at": utc_text(now),
            "expires_at": utc_text(expires),
        }
        body = _encode(canonical_json(claims).encode("utf-8"))
        signature = _encode(hmac.new(self._key, body.encode("ascii"), hashlib.sha256).digest())
        token = f"{body}.{signature}"
        with self._lock:
            self._active_by_session[source_session_id] = confirmation_id
        return ConfirmationCard(token, confirmation_id, claims["expires_at"])

    def verify(
        self,
        *,
        token: str,
        phrase: str,
        identity_id: str,
        source_session_id: str,
    ) -> dict[str, Any]:
        if phrase.strip() != CONFIRMATION_PHRASE:
            raise ContractError("CONFIRMATION_PHRASE_MISMATCH")
        try:
            body, signature = token.split(".", 1)
            expected = _encode(
                hmac.new(self._key, body.encode("ascii"), hashlib.sha256).digest()
            )
            if not hmac.compare_digest(signature, expected):
                raise ContractError("CONFIRMATION_TOKEN_INVALID")
            claims = json.loads(_decode(body))
        except ContractError:
            raise
        except Exception as exc:
            raise ContractError("CONFIRMATION_TOKEN_INVALID") from exc
        if not isinstance(claims, dict):
            raise ContractError("CONFIRMATION_TOKEN_INVALID")
        if claims.get("identity_id") != identity_id:
            raise ContractError("CONFIRMATION_IDENTITY_MISMATCH")
        if claims.get("source_session_id") != source_session_id:
            raise ContractError("CONFIRMATION_SESSION_MISMATCH")
        expires_at = claims.get("expires_at")
        if not isinstance(expires_at, str):
            raise ContractError("CONFIRMATION_TOKEN_INVALID")
        from datetime import datetime, timezone

        parsed = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        if parsed <= utc_now().astimezone(timezone.utc):
            raise ContractError("CONFIRMATION_TOKEN_EXPIRED")
        confirmation_id = claims.get("confirmation_id")
        with self._lock:
            active = self._active_by_session.get(source_session_id)
        if active != confirmation_id:
            raise ContractError("CONFIRMATION_TOKEN_SUPERSEDED")
        command = claims.get("command")
        if not isinstance(command, dict):
            raise ContractError("CONFIRMATION_TOKEN_INVALID")
        return claims

    def mark_consumed(self, source_session_id: str, confirmation_id: str) -> None:
        with self._lock:
            if self._active_by_session.get(source_session_id) == confirmation_id:
                del self._active_by_session[source_session_id]
