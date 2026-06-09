from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Any, Dict

from .models import CountryConfig, GatewayRequest


SUPPORTED_ACTIONS = {
    "list_projects",
    "list_workflows",
    "get_workflow",
    "online_workflow",
    "offline_workflow",
    "trigger_workflow",
    "list_instances",
    "get_instance",
}


def decode_payload_b64(payload_b64: str) -> Dict[str, Any]:
    if not payload_b64:
        return {}
    raw = base64.b64decode(payload_b64).decode("utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("payload must decode to a JSON object")
    return data


def load_countries_config() -> Dict[str, CountryConfig]:
    config_path = os.environ.get(
        "DS_COUNTRIES_CONFIG",
        str(Path(__file__).resolve().parent.parent / "config" / "countries.json"),
    )
    with open(config_path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)

    return {
        country: CountryConfig(country=country, **values)
        for country, values in raw.items()
    }


def validate_request(request: GatewayRequest, countries: Dict[str, CountryConfig]) -> None:
    if request.country not in countries:
        raise ValueError(f"unsupported country: {request.country}")
    if request.action not in SUPPORTED_ACTIONS:
        raise ValueError(f"unsupported action: {request.action}")
    if not request.ds_token:
        raise ValueError("ds_token is required")
