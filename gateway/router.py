from __future__ import annotations

from dataclasses import replace

from clients.dolphinscheduler_client import DolphinSchedulerClient
from gateway.models import CountryConfig, GatewayRequest
from handlers.workflow_handlers import dispatch_action


def _stringify(value):
    if value is None:
        return ""
    return str(value).strip()


def _apply_override(config: CountryConfig, override):
    if not override:
        return config

    allowed_fields = {
        "project_code",
        "tenant_code",
        "worker_group",
        "environment_code",
        "queue",
        "api_mode",
        "start_endpoint",
        "start_code_field",
    }
    updates = {key: value for key, value in override.items() if key in allowed_fields}
    if not updates:
        return config
    return replace(config, **updates)


def resolve_country_config(request: GatewayRequest, country_config: CountryConfig) -> CountryConfig:
    payload = request.payload or {}
    resolved = country_config

    resolved = _apply_override(
        resolved,
        country_config.action_overrides.get(request.action, {}),
    )

    project_code = _stringify(payload.get("project_code")) or resolved.project_code
    if project_code:
        resolved = _apply_override(
            resolved,
            country_config.project_overrides.get(project_code, {}),
        )

    workflow_code = _stringify(payload.get("workflow_code"))
    if workflow_code:
        resolved = _apply_override(
            resolved,
            country_config.workflow_overrides.get(workflow_code, {}),
        )

    payload_override = {
        key: payload[key]
        for key in (
            "project_code",
            "tenant_code",
            "worker_group",
            "environment_code",
            "queue",
            "api_mode",
            "start_endpoint",
            "start_code_field",
        )
        if key in payload and payload[key] not in ("", None)
    }
    return _apply_override(resolved, payload_override)


def route_request(request: GatewayRequest, country_config: CountryConfig):
    client = DolphinSchedulerClient(resolve_country_config(request, country_config), request.ds_token)
    return dispatch_action(client, request.action, request.payload)
