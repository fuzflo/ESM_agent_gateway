"""Owner-only CSV fields and committed asset references; never read remote URLs."""
from __future__ import annotations

from .contracts import validate_event_payload
from .errors import GatewayError
from .util import sha256_value

PRIVATE_FIELDS = ("primary_contact_email", "acquisition_campaign", "admin_import_note_short")
ASSET_TYPES = {"expert_agent_link": "expert_agent_artifact_id", "leapility_card_link": "leapility_card_artifact_id"}
ASSET_PUBLIC_FIELDS = ("artifact_id", "artifact_type", "display_title", "controlled_access_url",
                       "artifact_version", "version_role", "artifact_status", "link_access_status")


async def extend_owner_snapshot(service, state, snapshot):
    """Return availability separately from nullable values, with no Event JSON."""
    snapshot.update({field: None for field in PRIVATE_FIELDS})
    snapshot["import_assets"] = []
    snapshot["import_asset_status"] = {kind: "unavailable" for kind in ASSET_TYPES}
    snapshot["import_field_status"] = {field: "unavailable" for field in PRIVATE_FIELDS}
    snapshot["csv_profile_status"] = "unavailable"
    try:
        stream = await service.storage.stream_events(state.temp_application_id)
    except GatewayError:
        stream = None
    try:
        artifacts = await service.storage.find_artifacts(temp_application_id=state.temp_application_id)
    except GatewayError:
        artifacts = None
    snapshot["artifact_query_status"] = "available" if artifacts is not None else "unavailable"
    # Preserve existing legacy metadata, but scope it to the authenticated Owner.
    references = {a for e in (stream or []) for a in e.artifact_reference_ids}
    snapshot["artifact_versions"] = [
        {field: getattr(a, field) for field in ("artifact_id", "artifact_type", "version_role",
            "artifact_version", "artifact_status", "link_access_status")}
        for a in (artifacts or [])
        if a.temp_application_id == state.temp_application_id and a.esm_owner_id == state.esm_owner_id
        and a.artifact_status == "active" and a.artifact_type not in ASSET_TYPES
    ]
    if stream is None:
        return
    creates = [e for e in stream if e.event_type == "application_created"]
    if len(creates) != 1:
        snapshot["csv_profile_status"] = "invalid" if creates else "unavailable"
        return
    created = creates[0]
    if created.payload.get("entry_path") != "csv_import":
        snapshot["csv_profile_status"] = "not_applicable"
        snapshot["import_field_status"] = {field: "not_provided" for field in PRIVATE_FIELDS}
        snapshot["import_asset_status"] = {kind: "not_applicable" for kind in ASSET_TYPES}
        snapshot["artifact_versions"].extend(
            {field: getattr(a, field) for field in ("artifact_id", "artifact_type", "version_role",
                "artifact_version", "artifact_status", "link_access_status")}
            for a in (artifacts or []) if a.temp_application_id == state.temp_application_id
            and a.esm_owner_id == state.esm_owner_id and a.artifact_status == "active"
            and a.artifact_type in ASSET_TYPES and a.artifact_id in references)
        return
    try:
        validate_event_payload(created.event_type, created.payload)
        creator = service.identities.get_by_id(created.actor_id)
        expected_refs = {created.payload[f] for f in ASSET_TYPES.values() if created.payload[f]}
        valid = (created.temp_application_id == state.temp_application_id and created.stream_version == 1
                 and created.schema_version == "1.2.0" and created.actor_type == "admin"
                 and creator is not None and creator.is_admin
                 and created.esm_owner_id_at_event_time == state.esm_owner_id
                 and created.payload_hash == sha256_value(created.payload)
                 and len(expected_refs) == sum(bool(created.payload[f]) for f in ASSET_TYPES.values())
                 and set(created.artifact_reference_ids) == expected_refs)
    except GatewayError:
        valid = False
    if not valid:
        snapshot["csv_profile_status"] = "invalid"
        snapshot["import_field_status"] = {field: "invalid" for field in PRIVATE_FIELDS}
        snapshot["import_asset_status"] = {kind: "invalid" for kind in ASSET_TYPES}
        return
    for field in PRIVATE_FIELDS:
        snapshot[field] = created.payload[field]
        snapshot["import_field_status"][field] = "available" if created.payload[field] else "not_provided"
    for kind, field in ASSET_TYPES.items():
        expected_id = created.payload[field]
        if artifacts is None:
            snapshot["import_asset_status"][kind] = "unavailable" if expected_id else "not_provided"
            continue
        candidates = [a for a in artifacts if a.temp_application_id == state.temp_application_id
                      and a.esm_owner_id == state.esm_owner_id and a.artifact_type == kind
                      and a.artifact_status == "active"]
        if expected_id is None:
            snapshot["import_asset_status"][kind] = "invalid" if candidates else "not_provided"
            continue
        matches = [a for a in artifacts if a.artifact_id == expected_id]
        if not matches:
            snapshot["import_asset_status"][kind] = "unavailable"
            continue
        asset = matches[0]
        valid_asset = (len(matches) == 1 and len(candidates) == 1 and candidates[0].artifact_id == expected_id
                       and asset.temp_application_id == state.temp_application_id
                       and asset.esm_owner_id == state.esm_owner_id and asset.artifact_type == kind
                       and asset.artifact_status == "active" and asset.version_role == "source"
                       and asset.artifact_kind == "external_reference" and asset.stage == "agent_positioning"
                       and asset.artifact_schema_version == "1.2.0" and asset.artifact_version == 1
                       and asset.expert_name_snapshot == created.payload["expert_name"]
                       and asset.esm_owner_display_name == created.esm_owner_display_name_at_event_time
                       and (kind != "leapility_card_link" or asset.display_title == created.payload["expert_name"] + "的 Leapility Card")
                       and asset.access_policy == "owner_only" and asset.created_by_actor_type == "admin"
                       and asset.created_by_actor_id == created.actor_id
                       and asset.content_hash == sha256_value({"controlled_access_url": asset.controlled_access_url,
                           "display_title": asset.display_title, "readable_content_descriptor": None,
                           "logical_artifact_id": asset.logical_artifact_id, "artifact_version": 1}))
        if not valid_asset:
            snapshot["import_asset_status"][kind] = "invalid"
            continue
        snapshot["import_asset_status"][kind] = "available"
        snapshot["import_assets"].append({field: getattr(asset, field) for field in ASSET_PUBLIC_FIELDS})
        snapshot["artifact_versions"].append({field: getattr(asset, field) for field in
            ("artifact_id", "artifact_type", "version_role", "artifact_version", "artifact_status", "link_access_status")})
    statuses = set(snapshot["import_asset_status"].values())
    snapshot["csv_profile_status"] = "invalid" if "invalid" in statuses else "incomplete" if "unavailable" in statuses else "available"
