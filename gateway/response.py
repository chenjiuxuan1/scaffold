from __future__ import annotations

from typing import Any, Dict


def build_response(
    success: bool,
    country: str,
    action: str,
    request_id: str,
    data: Any = None,
    error: Any = None,
) -> Dict[str, Any]:
    return {
        "success": success,
        "country": country,
        "action": action,
        "request_id": request_id,
        "data": data,
        "error": error,
    }
