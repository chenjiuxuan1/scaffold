from __future__ import annotations

from clients.dolphinscheduler_client import DolphinSchedulerClient
from gateway.models import CountryConfig, GatewayRequest
from handlers.workflow_handlers import dispatch_action


def route_request(request: GatewayRequest, country_config: CountryConfig):
    client = DolphinSchedulerClient(country_config, request.ds_token)
    return dispatch_action(client, request.action, request.payload)
