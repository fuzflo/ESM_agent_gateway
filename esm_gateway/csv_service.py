"""CSV-only service path. Previews bind server-generated IDs and full row values."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import asdict

from .contracts import Artifact, validate_event_payload
from .csv_import import HEADERS, URL_FIELDS, normalize_url, parse_csv, validate_row
from .csv_report import build_report
from .errors import AuthorizationError, ContractError, GatewayError, PersistenceError
from .storage import EventDraft, artifacts_equivalent
from .util import new_id, new_temp_application_id, sha256_value, utc_text


class CSVImportService:
    def _csv_admin(self, raw_code):
        identity = self._require_identity(raw_code)
        if not identity.is_admin:
            raise AuthorizationError("ADMIN_ROLE_REQUIRED")
        return identity

    async def _csv_history(self):
        events, cursor = [], 0
        while True:
            page = await self.storage.events_after(cursor, limit=100)
            if not page:
                break
            events.extend(page)
            cursor = page[-1].global_sequence
        return events, await self.storage.list_all_artifacts()

    async def _csv_validate(self, csv_text, selected=None, ignore_temp_ids=()):
        parsed = parse_csv(csv_text)
        roster = {i.esm_owner_id: i for i in
                  (self.identities.get_by_id(r["esm_owner_id"]) for r in self.identities.public_roster())}
        rows = [validate_row(r, roster) for r in parsed]
        try:
            events, assets = await self._csv_history()
        except GatewayError:
            return {"status": "incomplete", "result_code": "CSV_CHECK_INCOMPLETE", "rows": rows,
                    "valid_rows": [], "duplicate_rows": [], "error_rows": [r for r in rows if r["errors"]],
                    "total_rows": len(rows), "message": "历史查询不可用，尚不能确认任何行为有效。"}
        events = [e for e in events if e.temp_application_id not in ignore_temp_ids]
        assets = [a for a in assets if a.temp_application_id not in ignore_temp_ids]
        references = {a for e in events for a in e.artifact_reference_ids}
        history = {f: {} for f in ("primary_contact_email", *URL_FIELDS)}
        names = set()
        for e in events:
            if e.event_type == "application_created":
                names.add(e.payload["expert_name"])
            for f in ("primary_contact_email", "primary_public_profile_url"):
                value = e.payload.get(f)
                if value:
                    try:
                        key = value.lower() if f == "primary_contact_email" else normalize_url(value)
                    except ValueError:
                        raise ContractError("CSV_HISTORY_INVALID")
                    history[f][key] = "historical_record"
        for a in assets:
            field = {"expert_agent_link": "expert_agent_url", "leapility_card_link": "leapility_card_url"}.get(a.artifact_type)
            if field:
                history[field][normalize_url(a.controlled_access_url)] = (
                    "historical_record" if a.artifact_id in references else "orphan_asset")
        local = {f: {} for f in history}
        local_names = {}
        for row in rows:
            if row["errors"] or (selected is not None and row["source_row"] not in selected):
                continue
            v = row["values"]
            local_names.setdefault(v["expert_name"], []).append(row["source_row"])
            for f in history:
                if v[f]:
                    key = v[f].lower() if f == "primary_contact_email" else normalize_url(v[f])
                    local[f].setdefault(key, []).append(row["source_row"])
        for row in rows:
            if row["errors"] or (selected is not None and row["source_row"] not in selected):
                continue
            v = row["values"]
            for f in history:
                if not v[f]:
                    continue
                key = v[f].lower() if f == "primary_contact_email" else normalize_url(v[f])
                if key in history[f]:
                    row["duplicates"].append({"column": f, "reason": history[f][key],
                        "message": "相同资料已存在；孤立资产须由维护者核实。" if history[f][key] == "orphan_asset" else "与已有记录重复，不覆盖已有专家。"})
                if len(local[f].get(key, [])) > 1:
                    row["duplicates"].append({"column": f, "reason": "within_file", "source_rows": local[f][key],
                                              "message": "文件内重复，请修正或只选择其中一行。"})
            if v["expert_name"] in names or len(local_names.get(v["expert_name"], [])) > 1:
                row["warnings"].append("same_name")
            if not any(v[f] for f in history):
                row["warnings"].append("no_strong_identity_fields")
        return {"status": "success", "result_code": "CSV_VALIDATED", "total_rows": len(rows), "rows": rows,
                "valid_rows": [r for r in rows if not r["errors"] and not r["duplicates"]],
                "duplicate_rows": [r for r in rows if r["duplicates"]],
                "error_rows": [r for r in rows if r["errors"]]}

    async def validate_csv_import(self, *, raw_code, csv_text):
        self._csv_admin(raw_code)
        try:
            result = await self._csv_validate(csv_text)
        except ContractError as exc:
            result = {"status": "error", "result_code": exc.result_code,
                      "file_errors": [{"error_code": exc.result_code, "message": exc.safe_message}],
                      "valid_rows": [], "duplicate_rows": [], "error_rows": []}
        result["counts"] = {"valid": len(result["valid_rows"]), "duplicate": len(result["duplicate_rows"]),
                            "error": len(result["error_rows"])}
        return {**result, "identity_verified": True, "credential_material_returned": False}

    async def _normalize_csv(self, identity, command):
        if not identity.is_admin:
            raise AuthorizationError("ADMIN_ROLE_REQUIRED")
        if set(command) - {"command_type", "csv_text", "selected_source_rows", "acknowledged_same_name_rows"}:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        selected = command.get("selected_source_rows")
        acknowledged = command.get("acknowledged_same_name_rows", [])
        for value in (selected, acknowledged):
            if not isinstance(value, list) or any(type(n) is not int for n in value) or len(set(value)) != len(value):
                raise ContractError("CSV_ROW_SELECTION_INVALID")
        if not selected or not set(acknowledged).issubset(selected):
            raise ContractError("CSV_ROW_SELECTION_INVALID")
        checked = await self._csv_validate(command.get("csv_text"), set(selected))
        if checked["status"] != "success":
            raise ContractError("CSV_CHECK_INCOMPLETE")
        by_row = {r["source_row"]: r for r in checked["rows"]}
        if not set(selected).issubset(by_row):
            raise ContractError("CSV_ROW_SELECTION_INVALID")
        batch_id = new_id("CSV")
        candidates = []
        for number in sorted(selected):
            row = by_row[number]
            if row["errors"] or row["duplicates"]:
                raise ContractError("CSV_SELECTED_ROW_INVALID", f"第 {number} 行存在错误或重复，请先修正或排除。")
            if "same_name" in row["warnings"] and number not in acknowledged:
                raise ContractError("CSV_SAME_NAME_ACK_REQUIRED", f"第 {number} 行同名，请明确核实为不同专家。")
            candidate = deepcopy(row)
            candidate["temp_application_id"] = new_temp_application_id()
            candidate["import_row_id"] = new_id("CSVROW")
            candidate["import_actor_id"] = identity.esm_owner_id
            candidate["row_fingerprint"] = sha256_value({"values": row["values"], "owner": row["owner_display_name"], "titles": row["asset_titles"]})
            candidate["artifacts"] = self._csv_assets(identity, candidate)
            candidate["payload"] = self._csv_payload(candidate, batch_id)
            validate_event_payload("application_created", candidate["payload"])
            candidates.append(candidate)
        # Keep excluded rows too. Selection-scoped duplicate checks must not erase
        # their original validation reasons from the final full-file report.
        all_checked = await self._csv_validate(command["csv_text"])
        if all_checked["status"] != "success":
            raise ContractError("CSV_CHECK_INCOMPLETE")
        return {"command_type": "import_experts_csv_batch", "skill": "esm-gateway-contract",
                "subject": batch_id, "import_batch_id": batch_id, "csv_text": command["csv_text"],
                "all_rows": all_checked["rows"], "import_actor_id": identity.esm_owner_id,
                "acknowledged_same_name_rows": acknowledged, "selected_source_rows": sorted(selected),
                "excluded_source_rows": sorted(set(by_row) - set(selected)), "candidates": candidates}, []

    async def _check_retry_records(self, candidates, context, expected_revision=None):
        # Absence is only an extra check: eligibility already requires a known
        # no-commit or never-started result, never an unknown/timeout result.
        events, assets = await self._csv_history()
        observed = False
        for candidate in candidates:
            if expected_revision is not None and (context["running"] or context["revision"] != expected_revision):
                raise ContractError("CSV_RETRY_STATE_CHANGED")
            own_events = [e for e in events if e.temp_application_id == candidate["temp_application_id"]]
            own_assets = [a for a in assets if a.temp_application_id == candidate["temp_application_id"]]
            if not own_events and not own_assets:
                continue
            observed = True
            result = next(r for r in context["results"] if r["source_row"] == candidate["source_row"])
            event = next((e for e in own_events if e.idempotency_key == candidate["import_row_id"] + ":create"), None)
            if event:
                result.update(event_id=event.event_id, event_commit_observed=True,
                              result_status="committed_pending", result_code="CSV_COMMIT_OBSERVED")
                await self._csv_readback(candidate, event, result)
            else:
                result.update(result_status="maintenance_required", result_code="CSV_RETRY_RECORD_CONFLICT")
        if observed:
            raise ContractError("CSV_RETRY_RECORD_EXISTS", "原标识下已有记录，须核实，不能重新创建。")

    async def _normalize_csv_retry(self, identity, command):
        if not identity.is_admin:
            raise AuthorizationError("ADMIN_ROLE_REQUIRED")
        if set(command) - {"command_type", "import_batch_id", "selected_source_rows", "acknowledged_same_name_rows"}:
            raise ContractError("COMMAND_PAYLOAD_INVALID")
        batch_id = command.get("import_batch_id")
        if not isinstance(batch_id, str):
            raise ContractError("CSV_BATCH_ID_INVALID")
        context = self._csv_batches.get(batch_id)
        if context is None:
            raise ContractError("CSV_CONTEXT_UNAVAILABLE", "可信批次上下文不可恢复，只能核实结果或交维护者。")
        original = context["command"]
        if original["import_actor_id"] != identity.esm_owner_id:
            raise AuthorizationError("CSV_BATCH_IDENTITY_MISMATCH")
        if context["running"]:
            raise ContractError("CSV_BATCH_RUNNING")
        revision = context["revision"]
        selected = command.get("selected_source_rows")
        ack = command.get("acknowledged_same_name_rows", [])
        for value in (selected, ack):
            if not isinstance(value, list) or any(type(n) is not int for n in value) or len(set(value)) != len(value):
                raise ContractError("CSV_ROW_SELECTION_INVALID")
        if not selected or not set(ack).issubset(selected):
            raise ContractError("CSV_ROW_SELECTION_INVALID")
        results = {r["source_row"]: r for r in context["results"]}
        if any(n not in results or results[n]["result_status"] not in {"failed_no_commit", "not_started"} for n in selected):
            raise ContractError("CSV_ROW_NOT_RETRYABLE")
        candidates = [deepcopy(c) for c in original["candidates"] if c["source_row"] in selected]
        await self._check_retry_records(candidates, context, expected_revision=revision)
        checked = await self._csv_validate(original["csv_text"], set(selected))
        if checked["status"] != "success":
            raise ContractError("CSV_CHECK_INCOMPLETE")
        acknowledged = sorted(set(original["acknowledged_same_name_rows"]) | set(ack))
        by_row = {r["source_row"]: r for r in checked["rows"]}
        for candidate in candidates:
            row = by_row[candidate["source_row"]]
            fingerprint = sha256_value({"values": row["values"], "owner": row["owner_display_name"], "titles": row["asset_titles"]})
            if fingerprint != candidate["row_fingerprint"]:
                raise ContractError("CSV_RETRY_CONTENT_CHANGED")
            if row["errors"] or row["duplicates"]:
                raise ContractError("CSV_SELECTED_ROW_INVALID")
            if "same_name" in row["warnings"] and row["source_row"] not in acknowledged:
                raise ContractError("CSV_SAME_NAME_ACK_REQUIRED")
        if context["running"] or context["revision"] != revision:
            raise ContractError("CSV_RETRY_STATE_CHANGED")
        normalized = deepcopy(original)
        normalized.update(command_type="retry_csv_import_batch", candidates=candidates,
                          selected_source_rows=sorted(selected), expected_revision=revision,
                          acknowledged_same_name_rows=acknowledged,
                          excluded_source_rows=sorted({r["source_row"] for r in original["all_rows"]} - set(selected)))
        return normalized, []

    def _csv_assets(self, identity, candidate):
        assets = []
        v = candidate["values"]
        for kind, field in (("expert_agent_link", "expert_agent_url"), ("leapility_card_link", "leapility_card_url")):
            if not v[field]:
                continue
            logical = new_id("LART")
            title = candidate["asset_titles"][kind]
            asset = Artifact(artifact_id=new_id("ART"), logical_artifact_id=logical,
                artifact_version=1, artifact_schema_version="1.2.0", artifact_type=kind,
                artifact_kind="external_reference", version_role="source", artifact_status="active",
                source_execution_id=None, source_result_hash=None, application_id=None,
                temp_application_id=candidate["temp_application_id"], expert_name_snapshot=v["expert_name"],
                esm_owner_id=v["esm_owner_id"], esm_owner_display_name=candidate["owner_display_name"],
                stage="agent_positioning", display_title=title, storage_type="controlled_external_url",
                content_page_or_object_id=None, controlled_access_url=v[field], mime_type=None, size_bytes=None,
                language=[], readable_content_descriptor=None,
                content_hash=sha256_value({"controlled_access_url": v[field], "display_title": title,
                    "readable_content_descriptor": None, "logical_artifact_id": logical, "artifact_version": 1}),
                created_at=utc_text(), created_by_actor_type="admin", created_by_actor_id=identity.esm_owner_id)
            assets.append(asdict(asset))
        return assets

    def _csv_payload(self, candidate, batch_id):
        v = candidate["values"]
        asset_ids = {a["artifact_type"]: a["artifact_id"] for a in candidate["artifacts"]}
        return {**{f: v[f] for f in ("expert_name", "source", "professional_domains", "market_regions")},
                **{f: v[f] or None for f in ("primary_contact_email", "acquisition_campaign", "primary_public_profile_url", "admin_import_note_short")},
                "entry_path": "csv_import", "expert_agent_artifact_id": asset_ids.get("expert_agent_link"),
                "leapility_card_artifact_id": asset_ids.get("leapility_card_link"), "csv_template_version": "1.2.0",
                "import_batch_id": batch_id, "import_row_id": candidate["import_row_id"],
                "source_row": candidate["source_row"], "row_fingerprint": candidate["row_fingerprint"]}

    async def _confirm_csv(self, identity, session, claims):
        if not identity.is_admin:
            raise AuthorizationError("ADMIN_ROLE_REQUIRED")
        command = claims["command"]
        batch_id = command["import_batch_id"]
        # Reserve synchronously before the first await; two sessions cannot
        # launch overlapping attempts or reuse a stale retry preview.
        if command["command_type"] == "retry_csv_import_batch":
            context = self._csv_batches.get(batch_id)
            if context is None:
                raise ContractError("CSV_CONTEXT_UNAVAILABLE")
            if context["running"] or context["revision"] != command["expected_revision"]:
                raise ContractError("CSV_RETRY_STATE_CHANGED")
            results = {r["source_row"]: r for r in context["results"]}
            if any(results[c["source_row"]]["result_status"] not in {"failed_no_commit", "not_started"} for c in command["candidates"]):
                raise ContractError("CSV_ROW_NOT_RETRYABLE")
            context["revision"] += 1
            context["running"] = True
            context.pop("error_code", None)
        else:
            context = {"command": deepcopy(command), "results": [], "running": True, "revision": 0}
            self._csv_batches[batch_id] = context
        task = asyncio.create_task(self._run_csv_locked(identity, session, claims, context))
        context["task"] = task
        self.confirmations.mark_consumed(session, str(claims["confirmation_id"]))
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=max(0.01, self.projection_wait_seconds))
        except asyncio.TimeoutError:
            pass
        return self._csv_result(batch_id, context)

    async def _run_csv_locked(self, identity, session, claims, context):
        async with self._write_lock:
            await self._run_csv(identity, session, claims, context)

    async def _run_csv(self, identity, session, claims, context):
        command = claims["command"]
        candidates = command["candidates"]
        pending = []
        try:
            if command["command_type"] == "retry_csv_import_batch":
                await self._check_retry_records(candidates, context)
            check = await self._csv_validate(command["csv_text"], set(command["selected_source_rows"]))
            if check["status"] != "success":
                raise ContractError("CSV_CHECK_INCOMPLETE")
            rows = {r["source_row"]: r for r in check["rows"]}
            stopped = False
            for candidate in candidates:
                if stopped:
                    continue
                result = self._csv_row_result(candidate)
                context["results"] = [r for r in context["results"] if r["source_row"] != candidate["source_row"]]
                context["results"].append(result)
                row = rows[candidate["source_row"]]
                fingerprint = sha256_value({"values": row["values"], "owner": row["owner_display_name"], "titles": row["asset_titles"]})
                if fingerprint != candidate["row_fingerprint"]:
                    result.update(result_status="validation_error", result_code="CSV_CONTENT_CHANGED")
                    continue
                if row["errors"] or row["duplicates"] or ("same_name" in row["warnings"] and row["source_row"] not in command["acknowledged_same_name_rows"]):
                    result.update(result_status="duplicate_blocked" if row["duplicates"] else "validation_error",
                                  result_code="CSV_CONFIRM_CONFLICT", reasons=row["errors"] + row["duplicates"] + row["warnings"])
                    continue
                owner = self.identities.get_by_id(candidate["values"]["esm_owner_id"])
                if owner is None or owner.display_name != candidate["owner_display_name"]:
                    result.update(result_status="validation_error", result_code="CSV_OWNER_CHANGED")
                    continue
                try:
                    phase = "artifact_create"
                    result.update(result_status="result_unverified", result_code="CSV_WRITE_IN_PROGRESS",
                                  event_commit_acknowledged=None)
                    for raw in candidate["artifacts"]:
                        receipt = await self.storage.put_artifact(Artifact(**raw))
                        result["asset_receipts"].append({"artifact_id": raw["artifact_id"], "artifact_type": raw["artifact_type"],
                            "commit_acknowledged": receipt.commit_acknowledged, "readback_verified": False})
                        if not receipt.commit_acknowledged:
                            raise PersistenceError("CSV_ASSET_ACK_UNKNOWN", write_may_have_succeeded=True)
                    result["artifact_commit_acknowledged"] = True if candidate["artifacts"] else None
                    phase = "event_append"
                    draft = EventDraft(event_type="application_created", temp_application_id=candidate["temp_application_id"],
                        actor_type="admin", actor_id=identity.esm_owner_id, esm_owner_id=owner.esm_owner_id,
                        esm_owner_display_name=owner.display_name, source_session_id=session, execution_id=None,
                        correlation_id=command["import_batch_id"], causation_id=str(claims["confirmation_id"]),
                        idempotency_key=candidate["import_row_id"] + ":create", command_hash=sha256_value(candidate["payload"]),
                        payload=candidate["payload"], artifact_reference_ids=[a["artifact_id"] for a in candidate["artifacts"]], occurred_at=utc_text())
                    event, _ = await self.storage.append_event(draft)
                    result.update(event_id=event.event_id, event_commit_acknowledged=True,
                                  result_status="committed_pending", result_code="CSV_COMMITTED_PENDING")
                    self.projector.notify()
                    pending.append((candidate, event, result))
                except GatewayError as exc:
                    any_asset = any(a["commit_acknowledged"] for a in result["asset_receipts"])
                    if isinstance(exc, PersistenceError) and exc.artifact_commit_acknowledged is True:
                        any_asset = True
                        if not any(a["artifact_id"] == exc.artifact_id for a in result["asset_receipts"]):
                            result["asset_receipts"].append({"artifact_id": exc.artifact_id,
                                "artifact_type": next((a["artifact_type"] for a in candidate["artifacts"] if a["artifact_id"] == exc.artifact_id), None),
                                "commit_acknowledged": True, "readback_verified": False})
                    unknown = isinstance(exc, PersistenceError) and (exc.write_may_have_succeeded or exc.event_commit_acknowledged is True)
                    known_partial = any_asset and (phase == "artifact_create" or (isinstance(exc, PersistenceError) and exc.event_commit_acknowledged is False))
                    state = "maintenance_required" if known_partial else "result_unverified" if unknown else "maintenance_required" if any_asset else "failed_no_commit"
                    if any_asset:
                        result["artifact_commit_acknowledged"] = True
                    result.update(result_status=state, result_code=exc.result_code)
                    if isinstance(exc, PersistenceError):
                        result["event_commit_acknowledged"] = False if phase == "artifact_create" else exc.event_commit_acknowledged
                        if state == "failed_no_commit":
                            result["event_commit_acknowledged"] = False
                        result["failed_phase"] = exc.failed_phase or phase
                        stopped = True
            if pending:
                await asyncio.gather(*(self._csv_wait_readback(c, e, r) for c, e, r in pending))
        except Exception as exc:
            code = exc.result_code if isinstance(exc, GatewayError) else "CSV_INTERNAL_ERROR"
            present = {r["source_row"] for r in context["results"]}
            for candidate in candidates:
                if candidate["source_row"] not in present:
                    context["results"].append(self._csv_row_result(candidate))
            context["error_code"] = code
            for result in context["results"]:
                if result["result_status"] == "result_unverified":
                    result["result_code"] = code
        finally:
            present = {r["source_row"] for r in context["results"]}
            context["results"].extend(self._csv_row_result(c) for c in candidates if c["source_row"] not in present)
            context["running"] = False

    def _csv_row_result(self, candidate):
        return {"source_row": candidate["source_row"], "expert_name": candidate["values"]["expert_name"],
                "esm_owner_id": candidate["values"]["esm_owner_id"], "esm_owner_display_name": candidate["owner_display_name"],
                "temp_application_id": candidate["temp_application_id"], "import_row_id": candidate["import_row_id"],
                "has_assets": bool(candidate["artifacts"]),
                "result_status": "not_started", "result_code": "CSV_NOT_STARTED", "event_id": None,
                "event_commit_acknowledged": False, "artifact_commit_acknowledged": None,
                "event_readback_verified": False, "artifact_readback_verified": False,
                "projection_readback_verified": False, "asset_receipts": [], "pending_phases": []}

    async def _csv_readback(self, candidate, event, result):
        try:
            stream = await self.storage.stream_events(candidate["temp_application_id"])
            actual = next((e for e in stream if e.event_id == event.event_id), None)
            if actual is not None:
                if (actual.payload != candidate["payload"] or actual.payload_hash != sha256_value(candidate["payload"])
                    or actual.esm_owner_id_at_event_time != candidate["values"]["esm_owner_id"]
                    or actual.esm_owner_display_name_at_event_time != candidate["owner_display_name"]
                    or actual.actor_type != "admin" or actual.actor_id != candidate["import_actor_id"]
                    or actual.command_hash != sha256_value(candidate["payload"])
                    or actual.schema_version != "1.2.0"
                    or set(actual.artifact_reference_ids) != {a["artifact_id"] for a in candidate["artifacts"]}):
                    result.update(result_status="maintenance_required", result_code="CSV_EVENT_MISMATCH")
                    return
                result["event_readback_verified"] = True
            asset_ok = True
            for raw in candidate["artifacts"]:
                stored = await self.storage.get_artifact(raw["artifact_id"])
                if stored is not None and not artifacts_equivalent(Artifact(**raw), stored):
                    result.update(result_status="maintenance_required", result_code="CSV_ASSET_MISMATCH")
                    return
                found = stored is not None
                asset_ok = asset_ok and found
                for receipt in result["asset_receipts"]:
                    if receipt["artifact_id"] == raw["artifact_id"]:
                        receipt["readback_verified"] = found
            result["artifact_readback_verified"] = asset_ok
            state = await self.storage.get_state(candidate["temp_application_id"])
            if state is not None and state.last_processed_stream_version >= event.stream_version:
                from .state_machine import replay_stream
                expected = replay_stream([e for e in stream if e.stream_version <= state.last_processed_stream_version])
                fields = ("temp_application_id", "esm_owner_id", "esm_owner_display_name", "expert_name", "source",
                          "professional_domains", "market_regions", "current_stage", "stage_status", "stage_outcome",
                          "last_applied_event_id", "last_processed_stream_version", "last_processed_global_sequence")
                projection_ok = expected is not None and not state.projection_conflict and all(
                    getattr(state, f) == getattr(expected, f) for f in fields)
                projection_ok = projection_ok and all(state.values.get(f) == expected.values.get(f)
                    for f in ("primary_public_profile_url", "expert_agent_artifact_id"))
                if not projection_ok:
                    result.update(result_status="maintenance_required", result_code="CSV_PROJECTION_MISMATCH")
                    return
                result["projection_readback_verified"] = True
        except ContractError as exc:
            result.update(result_status="maintenance_required", result_code=exc.result_code)
            return
        except GatewayError:
            pass
        result["pending_phases"] = [name for name in ("event_readback", "artifact_readback", "projection_readback") if not result[name + "_verified"]]
        if not result["pending_phases"]:
            result.update(result_status="success", result_code="CSV_IMPORT_COMPLETED")

    async def _csv_wait_readback(self, candidate, event, result):
        deadline = asyncio.get_running_loop().time() + self.projection_wait_seconds
        delay = 0.05
        while True:
            await self._csv_readback(candidate, event, result)
            if result["result_status"] != "committed_pending" or asyncio.get_running_loop().time() >= deadline:
                return
            self.projector.notify()
            await asyncio.sleep(min(delay, max(0, deadline - asyncio.get_running_loop().time())))
            delay = min(delay * 2, 5)

    def _csv_result(self, batch_id, context):
        results = deepcopy(context["results"])
        present = {r["source_row"] for r in results}
        results.extend(self._csv_row_result(c) for c in context["command"]["candidates"] if c["source_row"] not in present)
        counts = {}
        for r in results:
            counts[r["result_status"]] = counts.get(r["result_status"], 0) + 1
        all_results = {r["source_row"]: r for r in results}
        for row in context["command"]["all_rows"]:
            if row["source_row"] in all_results:
                continue
            state = "validation_error" if row["errors"] else "duplicate_blocked" if row["duplicates"] else "not_selected"
            all_results[row["source_row"]] = {"source_row": row["source_row"], "expert_name": row["values"]["expert_name"],
                "esm_owner_id": row["values"]["esm_owner_id"], "esm_owner_display_name": row["owner_display_name"],
                "result_status": state, "result_code": {"validation_error": "CSV_VALIDATION_ERROR", "duplicate_blocked": "CSV_DUPLICATE_BLOCKED", "not_selected": "CSV_NOT_SELECTED"}[state],
                "temp_application_id": None, "reasons": row["errors"] + row["duplicates"],
                "has_assets": bool(row["values"]["expert_agent_url"] or row["values"]["leapility_card_url"])}
        report_rows = sorted(all_results.values(), key=lambda r: r["source_row"])
        report_error = None
        try:
            report = build_report(batch_id, report_rows, selected_count=len(context["command"]["candidates"]),
                                  running=context["running"], revision=context["revision"])
        except Exception:
            # Document formatting must never disguise a committed database write
            # as a failed import. The same status call can regenerate the report.
            report = None
            report_error = "CSV_REPORT_UNAVAILABLE"
        return {"status": "pending" if context["running"] or counts.get("committed_pending") or counts.get("result_unverified") else "success" if counts.get("success") == len(results) else "partial_failure",
                "result_code": "CSV_IMPORT_RESULT", "identity_verified": True, "credential_material_returned": False,
                "import_batch_id": batch_id, "coverage": "complete", "running": context["running"],
                "results": sorted(results, key=lambda r: r["source_row"]), "counts": counts,
                "report_rows": report_rows, "report": report, "report_error": report_error,
                "excluded_source_rows": context["command"]["excluded_source_rows"], "error_code": context.get("error_code")}

    async def get_csv_import_status(self, *, raw_code, import_batch_id):
        self._csv_admin(raw_code)
        context = self._csv_batches.get(import_batch_id)
        if context is None:
            events, _ = await self._csv_history()
            found = [e for e in events if e.payload.get("import_batch_id") == import_batch_id]
            return {"status": "pending", "result_code": "CSV_CONTEXT_UNAVAILABLE", "identity_verified": True,
                    "credential_material_returned": False, "import_batch_id": import_batch_id,
                    "coverage": "partial" if found else "unknown", "event_ids": [e.event_id for e in found],
                    "message": "批次上下文不可恢复，仅核实到部分已提交事件；不要重复导入。"}
        if not context["running"]:
            for candidate in context["command"]["candidates"]:
                result = next(r for r in context["results"] if r["source_row"] == candidate["source_row"])
                if result["result_status"] == "committed_pending":
                    event = next((e for e in await self.storage.stream_events(candidate["temp_application_id"]) if e.event_id == result["event_id"]), None)
                    if event:
                        await self._csv_readback(candidate, event, result)
                elif result["result_status"] == "result_unverified":
                    event = await self.storage.find_event_by_idempotency(candidate["temp_application_id"], candidate["import_row_id"] + ":create")
                    if event:
                        result.update(event_id=event.event_id, result_status="committed_pending", result_code="CSV_COMMIT_OBSERVED")
                        self.projector.notify()
                        await self._csv_readback(candidate, event, result)
        return self._csv_result(import_batch_id, context)
