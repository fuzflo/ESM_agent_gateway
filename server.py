#!/usr/bin/env python3
"""One-endpoint Railway MCP service for ESM Agent V1."""

from __future__ import annotations

import asyncio
import hmac
import inspect
import json
import logging
import logging.config
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Annotated, Any, Awaitable, Callable

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import Field
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse

from esm_gateway.confirmation import ConfirmationService
from esm_gateway.errors import (
    AuthorizationError,
    ConfigurationError,
    ContractError,
    GatewayError,
    PersistenceError,
    StateTransitionError,
)
from esm_gateway.identity import IdentityDirectory
from esm_gateway.notion import NOTION_ENV_NAMES, storage_from_environment
from esm_gateway.projector import ProjectionWorker
from esm_gateway.service import ESMGatewayService
from esm_gateway.util import utc_text


GATEWAY_VERSION = "1.0.1"
MCP_PATH = os.environ.get("MCP_PATH", "/mcp")
BIND_HOST = os.environ.get("BIND_HOST", "0.0.0.0")  # noqa: S104
PUBLIC_PATHS = frozenset({"/", "/healthz"})
TOOL_ARGUMENTS = {
    "verify_esm_test_code": {"esm_test_code"},
    "get_esm_roster": {"esm_test_code"},
    "get_expert_snapshot": {"esm_test_code", "expert_name", "temp_application_id"},
    "prepare_write": {"esm_test_code", "source_session_id", "command"},
    "confirm_write": {
        "esm_test_code",
        "source_session_id",
        "confirmation_token",
        "confirmation_phrase",
    },
}

ESMTestCode = Annotated[
    str,
    Field(
        description=(
            "The exact ESM Test Code already supplied in this conversation. Pass it "
            "verbatim only to Gateway tools. Never display or log it."
        )
    ),
]
SourceSessionID = Annotated[
    str,
    Field(
        description=(
            "A stable opaque ID used for this Leapility conversation. Reuse it for all "
            "prepare/confirm calls in the conversation; do not use the test code."
        )
    ),
]
ConfirmationToken = Annotated[
    str,
    Field(description="Opaque token from the latest prepare_write card. Never display it."),
]
ConfirmationPhrase = Annotated[
    str,
    Field(description="The user's exact phrase: 确定以上所有信息，请你写入数据库"),
]


logger = logging.getLogger("esm-gateway")


class Runtime:
    def __init__(self) -> None:
        self.service: ESMGatewayService | None = None
        self.worker: ProjectionWorker | None = None
        self.schema_status: dict[str, str] | None = None
        self._identities: IdentityDirectory | None = None
        self._lock = asyncio.Lock()

    def identities(self) -> IdentityDirectory:
        if self._identities is not None:
            return self._identities
        identity_json = os.environ.get("ESM_IDENTITY_MAP_JSON", "").strip()
        if not identity_json:
            raise ConfigurationError("GATEWAY_CONFIGURATION_MISSING")
        self._identities = IdentityDirectory.from_json(identity_json)
        return self._identities

    async def start(self) -> ESMGatewayService:
        if self.service is not None:
            return self.service
        async with self._lock:
            if self.service is not None:
                return self.service
            signing_key = os.environ.get("CONFIRMATION_SIGNING_KEY", "").strip()
            if len(signing_key.encode("utf-8")) < 32:
                raise ConfigurationError("GATEWAY_CONFIGURATION_MISSING")
            identities = self.identities()
            storage = storage_from_environment()
            self.schema_status = await storage.preflight()
            worker = ProjectionWorker(
                storage,
                poll_interval_seconds=float(
                    os.environ.get("PROJECTION_POLL_INTERVAL_SECONDS", "1.0")
                ),
            )
            service = ESMGatewayService(
                identities=identities,
                confirmations=ConfirmationService(signing_key),
                storage=storage,
                projector=worker,
                projection_wait_seconds=float(
                    os.environ.get("PROJECTION_READBACK_WAIT_SECONDS", "0")
                ),
            )
            worker.start()
            self.worker = worker
            self.service = service
            logger.info("gateway_runtime_started schema_preflight=valid")
            return service

    async def stop(self) -> None:
        if self.worker is not None:
            await self.worker.stop()
        self.worker = None
        self.service = None
        logger.info("gateway_runtime_stopped")


runtime = Runtime()


def gateway_token() -> str:
    return os.environ.get("GATEWAY_TOKEN", "").strip()


def runtime_configuration_present() -> bool:
    required = (
        *NOTION_ENV_NAMES,
        "ESM_IDENTITY_MAP_JSON",
        "CONFIRMATION_SIGNING_KEY",
    )
    return bool(gateway_token()) and all(os.environ.get(name, "").strip() for name in required)


async def safe_call(
    operation: Callable[[ESMGatewayService], Awaitable[dict[str, Any]]]
    | Callable[[ESMGatewayService], dict[str, Any]],
) -> dict[str, Any]:
    try:
        service = await runtime.start()
        result = operation(service)
        if inspect.isawaitable(result):
            return await result
        return result
    except AuthorizationError as exc:
        level = logging.INFO if exc.result_code == "IDENTITY_DENIED" else logging.WARNING
        logger.log(level, "gateway_request_rejected error_code=%s", exc.result_code)
        return _failure(exc.result_code)
    except (ContractError, StateTransitionError) as exc:
        logger.warning("gateway_request_rejected error_code=%s", exc.result_code)
        return _failure(exc.result_code)
    except ConfigurationError as exc:
        logger.error("gateway_configuration_failed error_code=%s", exc.result_code)
        return _failure("GATEWAY_CONFIGURATION_ERROR")
    except PersistenceError as exc:
        logger.error("gateway_storage_failed error_code=%s", exc.result_code)
        return _failure(
            "GATEWAY_STORAGE_ERROR",
            write_may_have_succeeded=exc.write_may_have_succeeded,
            safe_retry_required=True,
        )
    except GatewayError as exc:
        logger.error("gateway_operation_failed error_code=%s", exc.result_code)
        return _failure(exc.result_code)
    except Exception:
        logger.exception("gateway_internal_failed error_code=GATEWAY_INTERNAL_ERROR")
        return _failure("GATEWAY_INTERNAL_ERROR")


def _failure(code: str, **values: Any) -> dict[str, Any]:
    return {
        "status": "failed",
        "result_code": code,
        "identity_verified": False if code in {"IDENTITY_DENIED", "GATEWAY_CONFIGURATION_ERROR"} else None,
        **values,
        "credential_material_returned": False,
    }


class StrictESMMCPServer(MCPServer):
    async def call_tool(
        self, name: str, arguments: dict[str, Any], context: Any = None
    ) -> Any:
        allowed = TOOL_ARGUMENTS.get(name)
        if allowed is not None and set(arguments) - allowed:
            raise ToolError(f"TOOL_CONTRACT_INVALID: {name} received unknown arguments")
        request_id = str(uuid.uuid4())
        started = time.monotonic()
        logger.info("gateway_tool_started request_id=%s tool=%s", request_id, name)
        try:
            result = await super().call_tool(name, arguments, context)
        except Exception:
            logger.exception(
                "gateway_tool_failed request_id=%s tool=%s error_code=TOOL_EXECUTION_ERROR",
                request_id,
                name,
            )
            raise
        structured = getattr(result, "structured_content", None)
        result_code = structured.get("result_code") if isinstance(structured, dict) else None
        logger.info(
            "gateway_tool_completed request_id=%s tool=%s result_code=%s duration_ms=%s",
            request_id,
            name,
            result_code or "RESULT_CODE_UNAVAILABLE",
            int((time.monotonic() - started) * 1000),
        )
        return result

    async def list_tools(self) -> list[Any]:
        tools = await super().list_tools()
        for tool in tools:
            if tool.name in TOOL_ARGUMENTS:
                tool.input_schema["additionalProperties"] = False
                if tool.output_schema is not None:
                    tool.output_schema["additionalProperties"] = False
        return tools


mcp = StrictESMMCPServer(
    name="esm-agent-v1-gateway",
    title="ESM Agent V1 Gateway",
    version=GATEWAY_VERSION,
    instructions=(
        "One controlled ESM V1 endpoint. Verify the test code first and wait for "
        "ESM_IDENTITY_VERIFIED before any snapshot or write tool. Formal writes always "
        "use prepare_write followed by the user's exact confirmation and confirm_write."
    ),
)


@mcp.tool()
async def verify_esm_test_code(esm_test_code: ESMTestCode) -> dict[str, Any]:
    """Verify the current conversation's test code before every business operation."""
    try:
        identity = runtime.identities().verify(esm_test_code)
    except ConfigurationError as exc:
        logger.error("gateway_configuration_failed error_code=%s", exc.result_code)
        return _failure("GATEWAY_CONFIGURATION_ERROR")
    if identity is None:
        logger.info("gateway_request_rejected error_code=IDENTITY_DENIED")
        return _failure("IDENTITY_DENIED")
    return {
        "status": "success",
        "result_code": "ESM_IDENTITY_VERIFIED",
        "identity_verified": True,
        "identity_id": identity.esm_owner_id,
        "identity_display_name": identity.display_name,
        "identity_role": identity.role,
        "credential_material_returned": False,
    }


@mcp.tool()
async def get_esm_roster(esm_test_code: ESMTestCode) -> dict[str, Any]:
    """Return the ten-person public roster to a verified ESM Admin for TDE assignment."""
    return await safe_call(lambda service: service.get_esm_roster(esm_test_code))


@mcp.tool()
async def get_expert_snapshot(
    esm_test_code: ESMTestCode,
    expert_name: Annotated[str, Field(description="Exact full expert name after trimming.")],
    temp_application_id: Annotated[
        str | None,
        Field(description="Required with the exact name only when duplicate exact names exist."),
    ] = None,
) -> dict[str, Any]:
    """Read an Owner or strictly limited cross-Owner snapshot by exact expert name."""
    return await safe_call(
        lambda service: service.get_snapshot(
            raw_code=esm_test_code,
            expert_name=expert_name,
            temp_application_id=temp_application_id,
        )
    )


@mcp.tool()
async def prepare_write(
    esm_test_code: ESMTestCode,
    source_session_id: SourceSessionID,
    command: Annotated[
        dict[str, Any],
        Field(
            description=(
                "A closed V1 command object; unknown command fields are rejected. "
                "For batch commands, professional_domains must use one or more of "
                "marketing, sales, product, operations, people_hr, finance, legal, "
                "healthcare, technology, education, creator_economy, other; "
                "market_regions must use one or more of na, latam, emea, apac, global, "
                "other. Map SEO to marketing and Mainland China to apac; never send "
                "human-facing translated labels as enum values."
            )
        ),
    ],
) -> dict[str, Any]:
    """Validate a stable preview and issue one ten-minute write card; does not write."""
    return await safe_call(
        lambda service: service.prepare_write(
            raw_code=esm_test_code,
            source_session_id=source_session_id,
            command=command,
        )
    )


@mcp.tool()
async def confirm_write(
    esm_test_code: ESMTestCode,
    source_session_id: SourceSessionID,
    confirmation_token: ConfirmationToken,
    confirmation_phrase: ConfirmationPhrase,
) -> dict[str, Any]:
    """Execute the latest payload-bound card and return per-write projection readback."""
    return await safe_call(
        lambda service: service.confirm_write(
            raw_code=esm_test_code,
            source_session_id=source_session_id,
            confirmation_token=confirmation_token,
            confirmation_phrase=confirmation_phrase,
        )
    )


@mcp.custom_route("/", methods=["GET"])
async def root(request: Request) -> PlainTextResponse:
    return PlainTextResponse(
        f"ESM Agent V1 Gateway v{GATEWAY_VERSION}\nMCP endpoint: {MCP_PATH}\n"
    )


@mcp.custom_route("/healthz", methods=["GET"])
async def healthz(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok" if runtime_configuration_present() else "misconfigured",
            "gateway_version": GATEWAY_VERSION,
            "server_time_utc": utc_text(),
            "gateway_token_configured": bool(gateway_token()),
            "runtime_configuration_present": runtime_configuration_present(),
            "runtime_started": runtime.service is not None,
            "schema_preflight_valid": runtime.schema_status is not None,
            "mcp_endpoint": MCP_PATH,
        }
    )


async def _deny(send: Any, status: int, code: str, message: str) -> None:
    body = json.dumps({"error": code, "message": message}).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode("ascii")),
                (b"www-authenticate", b'Bearer realm="esm-agent-v1-gateway"'),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


class BearerAuthMiddleware:
    def __init__(self, app: Any, *, public_paths: frozenset[str] = PUBLIC_PATHS) -> None:
        self.app = app
        self.public_paths = public_paths

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http" or scope.get("path", "") in self.public_paths:
            await self.app(scope, receive, send)
            return
        expected = gateway_token()
        if not expected:
            await _deny(send, 503, "gateway_not_configured", "Gateway access is not configured.")
            return
        presented = ""
        for key, value in scope.get("headers", []):
            if key.lower() == b"authorization":
                raw = value.decode("latin-1")
                scheme, _, candidate = raw.partition(" ")
                if scheme.lower() == "bearer":
                    presented = candidate.strip()
                break
        if not presented or not hmac.compare_digest(presented, expected):
            logger.info("gateway_http_identity_denied")
            await _deny(send, 401, "unauthorized", "Missing or invalid Gateway authorization.")
            return
        await self.app(scope, receive, send)


def _transport_security() -> TransportSecuritySettings:
    hosts = [item.strip() for item in os.environ.get("ALLOWED_HOSTS", "").split(",") if item.strip()]
    if not hosts:
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=hosts + [f"{host}:*" for host in hosts],
        allowed_origins=[f"https://{host}" for host in hosts],
    )


def build_app() -> Any:
    app = mcp.streamable_http_app(
        streamable_http_path=MCP_PATH,
        json_response=True,
        stateless_http=True,
        transport_security=_transport_security(),
        host=BIND_HOST,
    )
    mcp_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def combined_lifespan(starlette_app: Any):
        await runtime.start()
        try:
            async with mcp_lifespan(starlette_app) as state:
                yield state
        finally:
            await runtime.stop()

    app.router.lifespan_context = combined_lifespan
    app.add_middleware(BearerAuthMiddleware)
    return app


class DropBenignTermination(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return "Terminating session: None" not in record.getMessage()


def logging_config() -> dict[str, Any]:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {"drop_benign_termination": {"()": DropBenignTermination}},
        "formatters": {
            "default": {"format": "%(asctime)s %(levelname)s %(name)s %(message)s"},
            "access": {"format": "%(asctime)s %(levelname)s %(name)s %(message)s"},
        },
        "handlers": {
            "stdout": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": "default",
                "filters": ["drop_benign_termination"],
            },
            "access_stdout": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": "access",
                "filters": ["drop_benign_termination"],
            },
        },
        "loggers": {
            "uvicorn": {"handlers": ["stdout"], "level": level, "propagate": False},
            "uvicorn.error": {"handlers": ["stdout"], "level": level, "propagate": False},
            "uvicorn.access": {"handlers": ["access_stdout"], "level": level, "propagate": False},
        },
        "root": {"handlers": ["stdout"], "level": level},
    }


def main() -> None:
    logging.config.dictConfig(logging_config())
    logger.info(
        "gateway_starting version=%s access_configured=%s runtime_config_present=%s",
        GATEWAY_VERSION,
        bool(gateway_token()),
        runtime_configuration_present(),
    )
    import uvicorn

    uvicorn.run(
        build_app(),
        host=BIND_HOST,
        port=int(os.environ.get("PORT", "8000")),
        log_config=logging_config(),
        proxy_headers=True,
        forwarded_allow_ips="*",
    )


if __name__ == "__main__":
    main()
