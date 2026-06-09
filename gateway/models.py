from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class CountryConfig:
    country: str
    base_url: str
    project_code: str
    tenant_code: str = "default"
    worker_group: str = "default"
    environment_code: str = ""
    queue: str = ""
    api_mode: str = "auto"
    start_endpoint: str = "auto"
    start_code_field: str = "auto"
    action_overrides: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    project_overrides: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    workflow_overrides: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class GatewayRequest:
    country: str
    action: str
    ds_token: str
    request_id: str
    payload: Dict[str, Any]
