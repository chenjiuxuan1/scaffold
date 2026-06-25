from __future__ import annotations

from typing import Any, Dict, Tuple

from clients.dolphinscheduler_client import DolphinSchedulerClient


def dispatch_action(client: DolphinSchedulerClient, action: str, payload: Dict[str, Any]) -> Tuple[bool, Any]:
    handlers = {
        "list_projects": lambda: client.list_projects(payload),
        "list_workflows": lambda: client.list_workflows(payload),
        "list_schedules": lambda: client.list_schedules(payload),
        "get_schedule": lambda: client.get_schedule(payload),
        "create_schedule": lambda: client.create_schedule(payload),
        "update_schedule": lambda: client.update_schedule(payload),
        "online_schedule": lambda: client.online_schedule(payload),
        "offline_schedule": lambda: client.offline_schedule(payload),
        "schedule_blast_radius": lambda: client.schedule_blast_radius(payload),
        "get_workflow": lambda: client.get_workflow(payload),
        "online_workflow": lambda: client.release_workflow(payload, "ONLINE"),
        "offline_workflow": lambda: client.release_workflow(payload, "OFFLINE"),
        "trigger_workflow": lambda: client.trigger_workflow(payload),
        "list_instances": lambda: client.list_instances(payload),
        "get_instance": lambda: client.get_instance(payload),
        "retry_instance": lambda: client.retry_instance(payload),
        "append_task": lambda: client.append_task(payload),
        "append_sql_task": lambda: client.append_sql_task(payload),
        "append_shell_task": lambda: client.append_shell_task(payload),
        "disable_task": lambda: client.disable_task(payload),
        "disable_tasks_except": lambda: client.disable_tasks_except(payload),
        "delete_task": lambda: client.delete_task(payload),
        "dump_workflow_graph": lambda: client.dump_workflow_graph(payload),
        "list_datasources": lambda: client.list_datasources(payload),
        "get_datasource": lambda: client.get_datasource(payload),
        "extract_task_runtime_config": lambda: client.extract_task_runtime_config(payload),
    }
    return handlers[action]()
