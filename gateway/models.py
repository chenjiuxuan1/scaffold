from __future__ import annotations

from dataclasses import dataclass
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


@dataclass
class GatewayRequest:
    country: str
    action: str
    ds_token: str
    request_id: str
    payload: Dict[str, Any]
