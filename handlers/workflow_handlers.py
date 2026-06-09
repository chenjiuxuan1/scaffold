from __future__ import annotations

from typing import Any, Dict, Tuple

from clients.dolphinscheduler_client import DolphinSchedulerClient


def dispatch_action(client: DolphinSchedulerClient, action: str, payload: Dict[str, Any]) -> Tuple[bool, Any]:
    handlers = {
        "list_projects": lambda: client.list_projects(payload),
        "list_workflows": lambda: client.list_workflows(payload),
        "get_workflow": lambda: client.get_workflow(payload),
        "online_workflow": lambda: client.release_workflow(payload, "ONLINE"),
        "offline_workflow": lambda: client.release_workflow(payload, "OFFLINE"),
        "trigger_workflow": lambda: client.trigger_workflow(payload),
        "list_instances": lambda: client.list_instances(payload),
        "get_instance": lambda: client.get_instance(payload),
        "append_sql_task": lambda: client.append_sql_task(payload),
        "dump_workflow_graph": lambda: client.dump_workflow_graph(payload),
    }
    return handlers[action]()
