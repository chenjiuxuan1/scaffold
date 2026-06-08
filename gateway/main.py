from __future__ import annotations

from gateway.models import GatewayRequest
from gateway.response import build_response
from gateway.router import route_request
from gateway.utils import load_countries_config, validate_request


def execute_request(request: GatewayRequest):
    countries = load_countries_config()
    validate_request(request, countries)

    ok, result = route_request(request, countries[request.country])
    if ok:
        return build_response(
            True,
            request.country,
            request.action,
            request.request_id,
            data=result,
            error=None,
        )
    return build_response(
        False,
        request.country,
        request.action,
        request.request_id,
        data=None,
        error={"code": "DS_API_ERROR", "message": result},
    )
