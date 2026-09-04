"""Ten-minute, single-use, payload-bound confirmation cards."""

from __future__ import annotations

import copy
import hashlib
import hmac
import logging
import re
import secrets
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .errors import ContractError
from .util import new_id, utc_now, utc_text


CONFIRMATION_PHRASE = "确定以上所有信息，请你写入数据库"
CONFIRMATION_TTL_SECONDS = 600
OPAQUE_TOKEN_BYTES = 32
OPAQUE_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_-]{43}$")
TOKEN_TOMBSTONE_LIMIT = 4096

logger = logging.getLogger("esm-gateway.confirmation")


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
        self._cards_by_token_hash: dict[str, dict[str, Any]] = {}
        self._token_hash_by_confirmation_id: dict[str, str] = {}
        self._tombstones_by_token_hash: dict[str, dict[str, str]] = {}
        self._lock = threading.Lock()

    def _token_hash(self, token: str) -> str:
        return hmac.new(self._key, token.encode("utf-8"), hashlib.sha256).hexdigest()

    @staticmethod
    def _fingerprint(token_hash: str) -> str:
        return token_hash[:12]

    def _remove_card_locked(self, token_hash: str, status: str) -> None:
        claims = self._cards_by_token_hash.pop(token_hash, None)
        if claims is None:
            return
        confirmation_id = str(claims.get("confirmation_id", ""))
        source_session_id = str(claims.get("source_session_id", ""))
        self._token_hash_by_confirmation_id.pop(confirmation_id, None)
        if self._active_by_session.get(source_session_id) == confirmation_id:
            self._active_by_session.pop(source_session_id, None)
        self._tombstones_by_token_hash[token_hash] = {
            "identity_id": str(claims.get("identity_id", "")),
            "source_session_id": source_session_id,
            "status": status,
        }
        while len(self._tombstones_by_token_hash) > TOKEN_TOMBSTONE_LIMIT:
            oldest = next(iter(self._tombstones_by_token_hash))
            self._tombstones_by_token_hash.pop(oldest, None)

    def _prune_expired_locked(self) -> None:
        now = utc_now().astimezone(timezone.utc)
        expired_hashes: list[str] = []
        for token_hash, claims in self._cards_by_token_hash.items():
            expires_at = claims.get("expires_at")
            if not isinstance(expires_at, str):
                continue
            try:
                parsed = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            except ValueError:
                continue
            if parsed <= now:
                expired_hashes.append(token_hash)
        for token_hash in expired_hashes:
            self._remove_card_locked(token_hash, "expired")

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
        with self._lock:
            self._prune_expired_locked()
            previous_id = self._active_by_session.get(source_session_id)
            if previous_id is not None:
                previous_hash = self._token_hash_by_confirmation_id.get(previous_id)
                if previous_hash is not None:
                    self._remove_card_locked(previous_hash, "superseded")
            while True:
                token = secrets.token_urlsafe(OPAQUE_TOKEN_BYTES)
                token_hash = self._token_hash(token)
                if (
                    token_hash not in self._cards_by_token_hash
                    and token_hash not in self._tombstones_by_token_hash
                ):
                    break
            self._cards_by_token_hash[token_hash] = copy.deepcopy(claims)
            self._token_hash_by_confirmation_id[confirmation_id] = token_hash
            self._active_by_session[source_session_id] = confirmation_id
        logger.info(
            "confirmation_card_issued confirmation_id=%s token_length=%s token_fingerprint=%s",
            confirmation_id,
            len(token),
            self._fingerprint(token_hash),
        )
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
        token_hash = self._token_hash(token)
        if not OPAQUE_TOKEN_PATTERN.fullmatch(token):
            logger.warning(
                "confirmation_token_rejected reason=malformed token_length=%s "
                "token_fingerprint=%s",
                len(token),
                self._fingerprint(token_hash),
            )
            raise ContractError("CONFIRMATION_TOKEN_INVALID")
        with self._lock:
            self._prune_expired_locked()
            stored_claims = self._cards_by_token_hash.get(token_hash)
            tombstone = self._tombstones_by_token_hash.get(token_hash)
        if stored_claims is None:
            if tombstone is not None:
                if tombstone["identity_id"] != identity_id:
                    raise ContractError("CONFIRMATION_IDENTITY_MISMATCH")
                if tombstone["source_session_id"] != source_session_id:
                    raise ContractError("CONFIRMATION_SESSION_MISMATCH")
                if tombstone["status"] == "expired":
                    raise ContractError("CONFIRMATION_TOKEN_EXPIRED")
                raise ContractError("CONFIRMATION_TOKEN_SUPERSEDED")
            logger.warning(
                "confirmation_token_rejected reason=not_found token_length=%s "
                "token_fingerprint=%s",
                len(token),
                self._fingerprint(token_hash),
            )
            raise ContractError("CONFIRMATION_TOKEN_INVALID")
        claims = copy.deepcopy(stored_claims)
        if not isinstance(claims, dict):
            raise ContractError("CONFIRMATION_TOKEN_INVALID")
        if claims.get("identity_id") != identity_id:
            raise ContractError("CONFIRMATION_IDENTITY_MISMATCH")
        if claims.get("source_session_id") != source_session_id:
            raise ContractError("CONFIRMATION_SESSION_MISMATCH")
        expires_at = claims.get("expires_at")
        if not isinstance(expires_at, str):
            raise ContractError("CONFIRMATION_TOKEN_INVALID")
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
        logger.info(
            "confirmation_token_verified confirmation_id=%s token_length=%s "
            "token_fingerprint=%s",
            confirmation_id,
            len(token),
            self._fingerprint(token_hash),
        )
        return claims

    def mark_consumed(self, source_session_id: str, confirmation_id: str) -> None:
        with self._lock:
            token_hash = self._token_hash_by_confirmation_id.get(confirmation_id)
            claims = (
                self._cards_by_token_hash.get(token_hash)
                if token_hash is not None
                else None
            )
            if (
                token_hash is not None
                and claims is not None
                and claims.get("source_session_id") == source_session_id
            ):
                self._remove_card_locked(token_hash, "consumed")
