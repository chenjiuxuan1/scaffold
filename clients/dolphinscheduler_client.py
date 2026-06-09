from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Tuple

from gateway.models import CountryConfig


class DolphinSchedulerClient:
    def __init__(self, config: CountryConfig, ds_token: str) -> None:
        self.config = config
        self.ds_token = ds_token

    def request(
        self,
        method: str,
        path: str,
        query: Dict[str, Any] | None = None,
        form: Dict[str, Any] | None = None,
    ) -> Tuple[bool, Any]:
        query_string = ""
        if query:
            query_string = "?" + urllib.parse.urlencode(
                {k: v for k, v in query.items() if v not in ("", None)}
            )

        url = self.config.base_url.rstrip("/") + path + query_string
        headers = {
            "token": self.ds_token,
            "Accept": "application/json, text/plain, */*",
        }

        data = None
        if form is not None:
            data = urllib.parse.urlencode(
                {k: v for k, v in form.items() if v not in ("", None)}
            ).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        request = urllib.request.Request(url, data=data, headers=headers, method=method.upper())

        try:
            with urllib.request.urlopen(request, timeout=30) as resp:
                body = resp.read().decode("utf-8", errors="replace")
            try:
                return True, json.loads(body)
            except json.JSONDecodeError:
                return True, {"raw": body}
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = {"raw": body}
            return False, {"status": exc.code, "body": parsed, "url": url}
        except Exception as exc:
            return False, {"error": repr(exc), "url": url}

    def list_workflows(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        query = {
            "pageNo": payload.get("page_no", 1),
            "pageSize": payload.get("page_size", 20),
            "searchVal": payload.get("search_val", ""),
        }
        return self.request(
            "GET",
            f"/projects/{payload.get('project_code') or self.config.project_code}/workflow-definition",
            query=query,
        )

    def list_projects(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        query = {
            "pageNo": payload.get("page_no", 1),
            "pageSize": payload.get("page_size", 20),
            "searchVal": payload.get("search_val", ""),
        }
        return self.request(
            "GET",
            "/projects",
            query=query,
        )

    def get_workflow(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = payload.get("project_code") or self.config.project_code
        workflow_code = payload.get("workflow_code")
        if workflow_code:
            return self.request(
                "GET",
                f"/projects/{project_code}/workflow-definition/{workflow_code}",
            )

        ok, data = self.list_workflows(payload)
        if not ok:
            return ok, data

        workflow_name = str(payload.get("workflow_name", "")).strip()
        total_list = data.get("data", {}).get("totalList", [])
        for item in total_list:
            if str(item.get("name", "")).strip() == workflow_name:
                return True, item
        return False, {"message": f"workflow not found by name: {workflow_name}"}

    def _resolve_start_endpoint(self) -> str:
        endpoint = (self.config.start_endpoint or "auto").strip()
        if endpoint in ("", "auto"):
            return "start-process-instance"
        return endpoint

    def _resolve_start_code_field(self) -> str:
        field = (self.config.start_code_field or "auto").strip()
        if field in ("", "auto"):
            return "processDefinitionCode"
        return field

    def _candidate_start_modes(self) -> list[tuple[str, str]]:
        endpoint = (self.config.start_endpoint or "auto").strip()
        field = (self.config.start_code_field or "auto").strip()

        if endpoint not in ("", "auto") or field not in ("", "auto"):
            return [(self._resolve_start_endpoint(), self._resolve_start_code_field())]

        return [
            ("start-process-instance", "processDefinitionCode"),
            ("start-workflow-instance", "workflowDefinitionCode"),
        ]

    def release_workflow(self, payload: Dict[str, Any], release_state: str) -> Tuple[bool, Any]:
        project_code = payload.get("project_code") or self.config.project_code
        workflow_code = payload.get("workflow_code")
        return self.request(
            "POST",
            f"/projects/{project_code}/workflow-definition/{workflow_code}/release",
            query={"releaseState": release_state},
        )

    def trigger_workflow(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = payload.get("project_code") or self.config.project_code
        schedule_time = payload.get("schedule_time", "")
        launched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        form = {
            "failureStrategy": "CONTINUE",
            "warningType": "NONE",
            "warningGroupId": "0",
            "processInstancePriority": "MEDIUM",
            "workerGroup": self.config.worker_group,
            "environmentCode": self.config.environment_code,
            "tenantCode": self.config.tenant_code,
            "taskDependType": "TASK_ONLY" if payload.get("start_node_list") else "TASK_POST",
            "runMode": "RUN_MODE_SERIAL",
            "execType": "START_PROCESS",
            "dryRun": "0",
        }
        if self.config.queue:
            form["queue"] = self.config.queue
        if payload.get("start_node_list"):
            form["startNodeList"] = payload["start_node_list"]
        custom_params = payload.get("custom_params") or {}
        if custom_params:
            form["startParams"] = json.dumps(custom_params, ensure_ascii=False)
        attempts = []
        for endpoint_name, code_field in self._candidate_start_modes():
            attempt_form = deepcopy(form)
            attempt_form.pop("processDefinitionCode", None)
            attempt_form.pop("workflowDefinitionCode", None)
            attempt_form[code_field] = payload.get("workflow_code")
            attempt_form["scheduleTime"] = (
                schedule_time if schedule_time else launched_at
            ) if endpoint_name == "start-workflow-instance" else schedule_time

            ok, result = self.request(
                "POST",
                f"/projects/{project_code}/executors/{endpoint_name}",
                form=attempt_form,
            )
            if ok:
                return True, result
            attempts.append(
                {
                    "endpoint": endpoint_name,
                    "code_field": code_field,
                    "result": result,
                }
            )

        return False, {"message": "all trigger attempts failed", "attempts": attempts}

    def list_instances(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = payload.get("project_code") or self.config.project_code
        query = {
            "pageNo": payload.get("page_no", 1),
            "pageSize": payload.get("page_size", 20),
            "stateType": payload.get("state_type", ""),
            "searchVal": payload.get("search_val", ""),
        }
        return self.request(
            "GET",
            f"/projects/{project_code}/workflow-instances",
            query=query,
        )

    def get_instance(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = payload.get("project_code") or self.config.project_code
        instance_id = payload.get("instance_id")
        return self.request(
            "GET",
            f"/projects/{project_code}/workflow-instances/{instance_id}",
        )
