from __future__ import annotations

import json
import random
import re
import time
from copy import deepcopy
from datetime import datetime
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, Tuple

from gateway.models import CountryConfig


class DolphinSchedulerClient:
    RISKY_WORKFLOW_VARIABLES = {
        "src",
        "db",
        "dt",
        "full",
        "partition",
        "complement",
    }

    def __init__(self, config: CountryConfig, ds_token: str) -> None:
        self.config = config
        self.ds_token = ds_token

    def request(
        self,
        method: str,
        path: str,
        query: Dict[str, Any] | None = None,
        form: Dict[str, Any] | None = None,
        json_body: Dict[str, Any] | None = None,
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
        elif json_body is not None:
            data = json.dumps(json_body, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"

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

    def create_workflow(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        workflow_name = str(payload.get("workflow_name") or "").strip()
        if not project_code:
            return False, {"message": "project_code is required"}
        if not workflow_name:
            return False, {"message": "workflow_name is required"}

        ok, create_result, create_attempt = self._create_workflow_definition(project_code, payload)
        if not ok:
            return False, create_result
        workflow_code = self._extract_workflow_code(create_result)
        if not workflow_code:
            lookup_ok, workflow_result = self.get_workflow(
                {
                    "project_code": project_code,
                    "workflow_name": workflow_name,
                    "search_val": workflow_name,
                    "page_no": 1,
                    "page_size": 100,
                }
            )
            if lookup_ok and isinstance(workflow_result, dict):
                workflow_code = str(
                    workflow_result.get("code")
                    or workflow_result.get("workflowDefinitionCode")
                    or workflow_result.get("processDefinitionCode")
                    or ""
                ).strip()

        return True, {
            "project_code": project_code,
            "workflow_name": workflow_name,
            "workflow_code": workflow_code,
            "description": str(payload.get("description") or "").strip(),
            "tenant_code": str(payload.get("tenant_code") or self.config.tenant_code or "").strip(),
            "execution_type": str(payload.get("execution_type") or "PARALLEL").strip(),
            "timeout": self._safe_int(payload.get("timeout")),
            "global_params": self._normalize_json_value(payload.get("global_params"), default=[]),
            "task_definition_count": 0,
            "task_relation_count": 0,
            "location_count": 0,
            "create_attempt": create_attempt,
            "create_result": create_result,
        }

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

    def list_schedules(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = payload.get("project_code") or self.config.project_code
        query = {
            "pageNo": payload.get("page_no", 1),
            "pageSize": payload.get("page_size", 200),
            "searchVal": payload.get("search_val", ""),
        }
        ok, result = self.request(
            "GET",
            f"/projects/{project_code}/schedules",
            query=query,
        )
        if not ok:
            return ok, result

        workflow_code = str(payload.get("workflow_code") or "").strip()
        schedule_id = str(payload.get("schedule_id") or "").strip()
        if not workflow_code and not schedule_id:
            return True, result

        total_list = result.get("data", {}).get("totalList", [])
        if not isinstance(total_list, list):
            total_list = []

        filtered = []
        for item in total_list:
            item_schedule_id = str(item.get("id") or item.get("scheduleId") or "").strip()
            item_workflow_code = str(
                item.get("processDefinitionCode")
                or item.get("workflowDefinitionCode")
                or ""
            ).strip()
            if schedule_id and item_schedule_id != schedule_id:
                continue
            if workflow_code and item_workflow_code != workflow_code:
                continue
            filtered.append(item)

        wrapped = deepcopy(result)
        if isinstance(wrapped.get("data"), dict):
            wrapped["data"]["totalList"] = filtered
            wrapped["data"]["total"] = len(filtered)
        return True, wrapped

    def get_schedule(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        schedule_id = str(payload.get("schedule_id") or "").strip()
        workflow_code = str(payload.get("workflow_code") or "").strip()
        workflow_name = str(payload.get("workflow_name") or "").strip()

        if schedule_id:
            path = f"/projects/{project_code}/schedules/{schedule_id}"
            attempts = []
            for method in ("GET",):
                ok, result = self.request(method, path)
                if ok:
                    return True, result
                attempts.append({"method": method, "result": result})

        ok, list_result = self.list_schedules(
            {
                "project_code": project_code,
                "workflow_code": workflow_code,
                "schedule_id": schedule_id,
                "search_val": payload.get("search_val", ""),
                "page_no": 1,
                "page_size": 200,
            }
        )
        if not ok:
            return False, list_result

        items = list_result.get("data", {}).get("totalList", [])
        if not isinstance(items, list):
            items = []
        if schedule_id:
            for item in items:
                if str(item.get("id") or item.get("scheduleId") or "").strip() == schedule_id:
                    return True, item
        if workflow_code:
            for item in items:
                if str(item.get("processDefinitionCode") or item.get("workflowDefinitionCode") or "").strip() == workflow_code:
                    return True, item
        if workflow_name:
            for item in items:
                if str(item.get("processDefinitionName") or item.get("workflowDefinitionName") or "").strip() == workflow_name:
                    return True, item
        return False, {
            "message": "schedule not found",
            "project_code": project_code,
            "schedule_id": schedule_id,
            "workflow_code": workflow_code,
            "workflow_name": workflow_name,
        }

    def create_schedule(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        workflow_code = str(payload.get("workflow_code") or "").strip()
        if not project_code:
            return False, {"message": "project_code is required"}
        if not workflow_code:
            return False, {"message": "workflow_code is required"}

        forms = self._build_schedule_forms(payload, project_code=project_code, workflow_code=workflow_code)
        attempts = []
        for form_label, form in forms:
            ok, result = self.request("POST", f"/projects/{project_code}/schedules", form=form)
            if ok and self._is_ds_success(result):
                if isinstance(result, dict):
                    result = {**result, "form_label": form_label}
                return True, result
            attempts.append({"form_label": form_label, "result": result})
        return False, {
            "message": "all create_schedule attempts failed",
            "project_code": project_code,
            "workflow_code": workflow_code,
            "attempts": attempts,
        }

    def update_schedule(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        workflow_code = str(payload.get("workflow_code") or "").strip()
        schedule_id = self._resolve_schedule_id(payload)
        if not schedule_id:
            if workflow_code:
                return False, {
                    "message": "schedule not found for workflow_code",
                    "project_code": project_code,
                    "workflow_code": workflow_code,
                }
            return False, {"message": "schedule_id or workflow_code is required"}
        forms = self._build_schedule_forms(payload, project_code=project_code, workflow_code=workflow_code)
        attempts = []
        for form_label, form in forms:
            ok, result = self.request("PUT", f"/projects/{project_code}/schedules/{schedule_id}", form=form)
            if ok and self._is_ds_success(result):
                if isinstance(result, dict):
                    result = {**result, "form_label": form_label}
                return True, result
            attempts.append({"form_label": form_label, "result": result})
        return False, {
            "message": "all update_schedule attempts failed",
            "project_code": project_code,
            "workflow_code": workflow_code,
            "schedule_id": schedule_id,
            "attempts": attempts,
        }

    def online_schedule(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        schedule_id = self._resolve_schedule_id(payload)
        if not schedule_id:
            workflow_code = str(payload.get("workflow_code") or "").strip()
            if workflow_code:
                return False, {
                    "message": "schedule not found for workflow_code",
                    "project_code": str(payload.get("project_code") or self.config.project_code).strip(),
                    "workflow_code": workflow_code,
                }
            return False, {"message": "schedule_id or workflow_code is required"}
        ok, result = self.release_schedule(payload, schedule_id, "ONLINE")
        if not ok:
            return False, result
        return self._finalize_schedule_release(payload, schedule_id, "ONLINE", result)

    def offline_schedule(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        schedule_id = self._resolve_schedule_id(payload)
        if not schedule_id:
            workflow_code = str(payload.get("workflow_code") or "").strip()
            if workflow_code:
                return False, {
                    "message": "schedule not found for workflow_code",
                    "project_code": str(payload.get("project_code") or self.config.project_code).strip(),
                    "workflow_code": workflow_code,
                }
            return False, {"message": "schedule_id or workflow_code is required"}
        ok, result = self.release_schedule(payload, schedule_id, "OFFLINE")
        if not ok:
            return False, result
        return self._finalize_schedule_release(payload, schedule_id, "OFFLINE", result)

    def schedule_blast_radius(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        ok, workflow_result = self.get_workflow(payload)
        if not ok:
            return False, workflow_result

        detail = self._unwrap_workflow_detail(workflow_result)
        schedule_summary = self._resolve_schedule_summary(project_code=project_code, workflow_detail=detail)
        workflow_meta = self._get_workflow_meta(detail)
        return True, {
            "project_code": project_code,
            "workflow_code": str(
                payload.get("workflow_code")
                or workflow_meta.get("code")
                or detail.get("workflowDefinitionCode")
                or ""
            ).strip(),
            "workflow_name": str(
                workflow_meta.get("name")
                or detail.get("workflowDefinitionName")
                or ""
            ).strip(),
            "workflow_release_state": str(workflow_meta.get("releaseState") or "").strip(),
            "schedule_summary": schedule_summary,
            "blast_radius": {
                "shared_parent_workflow_detected": False,
                "shared_schedule_detected": False,
                "manual_review_required": False,
                "notes": [
                    "schedule_blast_radius 当前基于 workflow detail 和 schedules 列表判断",
                    "如涉及共享父工作流编排，仍建议人工复核 DAG 与上游触发链路",
                ],
            },
        }

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

    def dump_workflow_graph(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        ok, workflow_result = self.get_workflow(payload)
        if not ok:
            return False, workflow_result

        detail = self._unwrap_workflow_detail(workflow_result)
        if not detail:
            return False, {"message": "workflow detail payload is empty", "raw": workflow_result}

        task_definitions = self._get_workflow_task_definitions(detail)
        task_relations = self._get_workflow_task_relations(detail)
        locations = self._get_workflow_locations(detail)
        workflow_meta = self._get_workflow_meta(detail)

        return True, {
            "workflow_summary": {
                "name": workflow_meta.get("name"),
                "project_code": str(payload.get("project_code") or self.config.project_code).strip(),
                "workflow_code": str(payload.get("workflow_code") or workflow_meta.get("code") or "").strip(),
                "release_state": workflow_meta.get("releaseState") or workflow_meta.get("scheduleReleaseState") or workflow_meta.get("release_state"),
                "tenant_code": detail.get("tenantCode"),
                "execution_type": workflow_meta.get("executionType"),
                "timeout": workflow_meta.get("timeout"),
            },
            "counts": {
            "task_definition_count": len(task_definitions),
            "task_relation_count": len(task_relations),
            "location_count": len(locations),
        },
            "schedule_summary": self._resolve_schedule_summary(
                project_code=str(payload.get("project_code") or self.config.project_code).strip(),
                workflow_detail=detail,
            ),
            "task_definitions": task_definitions,
            "task_relations": task_relations,
            "locations": locations,
            "raw_workflow_detail": detail,
        }

    def _resolve_start_endpoint(self) -> str:
        endpoint = (self.config.start_endpoint or "auto").strip()
        if endpoint in ("", "auto"):
            return "start-process-instance"
        return endpoint

    def _execute_instance_action(
        self,
        payload: Dict[str, Any],
        execute_type: str,
        action_name: str,
    ) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        instance_id = self._safe_int(payload.get("instance_id"))
        if not project_code:
            return False, {"message": f"{action_name} requires project_code"}
        if instance_id <= 0:
            return False, {"message": f"{action_name} requires instance_id"}

        ok, result = self.request(
            "POST",
            f"/projects/{project_code}/executors/execute",
            form={
                "workflowInstanceId": instance_id,
                "executeType": execute_type,
            },
        )
        if not ok or not self._is_ds_success(result):
            return False, result
        return True, {
            "project_code": project_code,
            "instance_id": instance_id,
            "execute_type": execute_type,
            "result": result,
        }

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

    def release_schedule(
        self,
        payload: Dict[str, Any],
        schedule_id: str | int,
        release_state: str,
    ) -> Tuple[bool, Any]:
        project_code = payload.get("project_code") or self.config.project_code
        attempts = []
        normalized_release_state = str(release_state or "").strip().upper()
        suffix = "online" if normalized_release_state == "ONLINE" else "offline"
        candidate_requests = [
            (
                "POST",
                f"/projects/{project_code}/schedules/{schedule_id}/{suffix}",
                None,
            ),
            (
                "POST",
                f"/projects/{project_code}/schedules/{schedule_id}/release",
                {"releaseState": normalized_release_state},
            ),
            (
                "GET",
                f"/projects/{project_code}/schedules/{schedule_id}/release",
                {"releaseState": normalized_release_state},
            ),
            (
                "PUT",
                f"/projects/{project_code}/schedules/{schedule_id}/release",
                {"releaseState": normalized_release_state},
            ),
        ]
        for method, path, query in candidate_requests:
            ok, result = self.request(
                method,
                path,
                query=query,
            )
            if ok and self._is_ds_success(result):
                return True, result
            attempts.append({"method": method, "path": path, "query": query, "result": result})
            status = result.get("status") if isinstance(result, dict) else None
            if status not in (405, 404):
                break
        return False, {
            "message": "all schedule release attempts failed",
            "schedule_id": str(schedule_id),
            "release_state": normalized_release_state,
            "attempts": attempts,
        }

    def _finalize_schedule_release(
        self,
        payload: Dict[str, Any],
        schedule_id: str | int,
        release_state: str,
        release_result: Any,
    ) -> Tuple[bool, Any]:
        wait_result = self._wait_for_schedule_release_state(
            project_code=str(payload.get("project_code") or self.config.project_code).strip(),
            schedule_id=str(schedule_id),
            expected_release_state=str(release_state or "").strip().upper(),
        )
        return True, {
            "schedule_id": str(schedule_id),
            "release_state": str(release_state or "").strip().upper(),
            "release_result": release_result,
            **wait_result,
        }

    def _wait_for_schedule_release_state(
        self,
        *,
        project_code: str,
        schedule_id: str,
        expected_release_state: str,
        max_attempts: int = 8,
        interval_seconds: float = 1.0,
    ) -> Dict[str, Any]:
        last_schedule = None
        for attempt in range(1, max_attempts + 1):
            ok, schedule = self.get_schedule(
                {
                    "project_code": project_code,
                    "schedule_id": schedule_id,
                }
            )
            if ok and isinstance(schedule, dict):
                last_schedule = schedule
                observed_state = str(schedule.get("releaseState") or schedule.get("scheduleReleaseState") or "").strip().upper()
                if observed_state == expected_release_state:
                    return {
                        "consistency_confirmed": True,
                        "consistency_attempts": attempt,
                        "schedule": schedule,
                    }
            if attempt < max_attempts:
                time.sleep(interval_seconds)
        return {
            "consistency_confirmed": False,
            "consistency_attempts": max_attempts,
            "schedule": last_schedule,
            "warning": f"schedule release state did not converge to {expected_release_state} within wait window",
        }

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

    def list_task_instances(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = payload.get("project_code") or self.config.project_code
        process_instance_id = (
            payload.get("process_instance_id")
            or payload.get("instance_id")
            or payload.get("workflow_instance_id")
        )
        query = {
            "pageNo": payload.get("page_no", 1),
            "pageSize": payload.get("page_size", 100),
            "processInstanceId": process_instance_id,
            "stateType": payload.get("state_type", ""),
            "searchVal": payload.get("search_val", ""),
        }
        return self.request(
            "GET",
            f"/projects/{project_code}/task-instances",
            query=query,
        )

    def get_task_log(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        if not project_code:
            return False, {"message": "project_code is required"}

        task_instance = None
        task_instance_id = self._safe_int(payload.get("task_instance_id"))
        if task_instance_id > 0:
            task_instance = {
                "id": task_instance_id,
                "taskInstanceId": task_instance_id,
                "processInstanceId": self._safe_int(
                    payload.get("process_instance_id") or payload.get("instance_id")
                ),
                "name": str(payload.get("task_name") or "").strip(),
                "taskCode": self._safe_int(payload.get("task_code")),
                "state": str(payload.get("state") or "").strip(),
                "host": str(payload.get("host") or "").strip(),
                "logPath": str(payload.get("log_path") or "").strip(),
            }
        else:
            ok, resolved = self._resolve_task_instance(payload)
            if not ok:
                return False, resolved
            task_instance = resolved
            task_instance_id = self._safe_int(
                task_instance.get("id") or task_instance.get("taskInstanceId")
            )

        if task_instance_id <= 0:
            return False, {"message": "task_instance_id could not be resolved"}

        attempts = [
            (
                "project_task_instance_log",
                lambda: self.request(
                    "GET",
                    f"/projects/{project_code}/task-instances/{task_instance_id}/log",
                ),
            ),
            (
                "log_detail",
                lambda: self.request(
                    "GET",
                    "/log/detail",
                    query={
                        "taskInstanceId": task_instance_id,
                        "skipLineNum": self._safe_int(payload.get("skip_line_num"), 0),
                        "limit": self._safe_int(payload.get("limit"), 2000),
                    },
                ),
            ),
        ]

        failed_attempts = []
        for endpoint_name, runner in attempts:
            ok, result = runner()
            if not ok:
                failed_attempts.append({"endpoint": endpoint_name, "result": result})
                continue
            if not self._is_ds_success(result):
                failed_attempts.append({"endpoint": endpoint_name, "result": result})
                continue
            log_text = self._extract_task_log_text(result)
            if self._looks_like_html_document(log_text):
                failed_attempts.append(
                    {
                        "endpoint": endpoint_name,
                        "result": result,
                        "reason": "html_document_returned",
                    }
                )
                continue
            return True, {
                "project_code": project_code,
                "task_instance_id": task_instance_id,
                "process_instance_id": self._safe_int(task_instance.get("processInstanceId")),
                "task_name": str(
                    task_instance.get("name")
                    or task_instance.get("taskName")
                    or ""
                ).strip(),
                "task_code": self._safe_int(task_instance.get("taskCode")),
                "state": str(task_instance.get("state") or "").strip(),
                "host": str(task_instance.get("host") or "").strip(),
                "log_path": str(
                    task_instance.get("logPath")
                    or task_instance.get("log_path")
                    or ""
                ).strip(),
                "log_endpoint_used": endpoint_name,
                "log": log_text,
                "raw_result": result,
                "task_instance": task_instance,
            }

        return False, {
            "message": "failed to fetch task log",
            "project_code": project_code,
            "task_instance_id": task_instance_id,
            "attempts": failed_attempts,
        }

    def get_instance(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = payload.get("project_code") or self.config.project_code
        instance_id = payload.get("instance_id")
        return self.request(
            "GET",
            f"/projects/{project_code}/workflow-instances/{instance_id}",
        )

    def retry_instance(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        return self._execute_instance_action(
            payload=payload,
            execute_type="START_FAILURE_TASK_PROCESS",
            action_name="retry_instance",
        )

    def list_datasources(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        query = {
            "pageNo": payload.get("page_no", 1),
            "pageSize": payload.get("page_size", 200),
            "searchVal": payload.get("search_val", ""),
        }
        return self.request("GET", "/datasources", query=query)

    def get_datasource(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        datasource_id = str(payload.get("datasource_id") or "").strip()
        datasource = str(payload.get("datasource") or payload.get("datasource_name") or "").strip()

        if datasource_id:
            return self.request("GET", f"/datasources/{datasource_id}")

        ok, result = self.list_datasources(payload)
        if not ok:
            return False, result
        total_list = result.get("data", {}).get("totalList", [])
        if not isinstance(total_list, list):
            total_list = []
        for item in total_list:
            item_name = str(item.get("name") or item.get("datasourceName") or "").strip()
            if datasource and item_name == datasource:
                return True, item
        return False, {"message": f"datasource not found: {datasource}"}

    def _resolve_task_instance(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        process_instance_id = self._safe_int(
            payload.get("process_instance_id")
            or payload.get("instance_id")
            or payload.get("workflow_instance_id")
        )
        task_name = str(payload.get("task_name") or "").strip()
        task_code = self._safe_int(payload.get("task_code"))
        if process_instance_id <= 0:
            return False, {
                "message": "get_task_log requires task_instance_id or process_instance_id/instance_id",
            }
        if not task_name and task_code <= 0:
            return False, {
                "message": "get_task_log requires task_name or task_code when task_instance_id is absent",
            }

        ok, result = self.list_task_instances(
            {
                "project_code": payload.get("project_code") or self.config.project_code,
                "instance_id": process_instance_id,
                "page_no": 1,
                "page_size": max(self._safe_int(payload.get("page_size")), 100) or 100,
                "state_type": payload.get("state_type", ""),
                "search_val": payload.get("search_val", ""),
            }
        )
        if not ok:
            return False, result

        items = self._extract_total_list(result)
        if not items:
            return False, {
                "message": "no task instances found for process instance",
                "process_instance_id": process_instance_id,
            }

        for item in items:
            item_name = str(item.get("name") or item.get("taskName") or "").strip()
            item_task_code = self._safe_int(item.get("taskCode"))
            if task_name and item_name == task_name:
                return True, item
            if task_code > 0 and item_task_code == task_code:
                return True, item
        return False, {
            "message": "task instance not found in process instance",
            "process_instance_id": process_instance_id,
            "task_name": task_name,
            "task_code": task_code if task_code > 0 else "",
            "candidates": [
                {
                    "task_instance_id": self._safe_int(item.get("id") or item.get("taskInstanceId")),
                    "task_name": str(item.get("name") or item.get("taskName") or "").strip(),
                    "task_code": self._safe_int(item.get("taskCode")),
                    "state": str(item.get("state") or "").strip(),
                }
                for item in items
            ],
        }

    def _extract_total_list(self, result: Any) -> list[Dict[str, Any]]:
        if isinstance(result, dict):
            data = result.get("data")
            if isinstance(data, dict):
                total_list = data.get("totalList")
                if isinstance(total_list, list):
                    return [item for item in total_list if isinstance(item, dict)]
                records = data.get("records")
                if isinstance(records, list):
                    return [item for item in records if isinstance(item, dict)]
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
        if isinstance(result, list):
            return [item for item in result if isinstance(item, dict)]
        return []

    def _extract_task_log_text(self, result: Any) -> str:
        if isinstance(result, dict):
            data = result.get("data")
            if isinstance(data, dict):
                for key in ("log", "content", "rawLog", "message", "msg"):
                    value = data.get(key)
                    if isinstance(value, str):
                        return value
            if isinstance(data, str):
                return data
            for key in ("raw", "message", "msg"):
                value = result.get(key)
                if isinstance(value, str):
                    return value
        if isinstance(result, str):
            return result
        return ""

    def _looks_like_html_document(self, text: str) -> bool:
        normalized = str(text or "").lstrip().lower()
        if not normalized:
            return False
        probe = normalized[:1000]
        return "<!doctype html" in probe or "<html" in probe

    def extract_task_runtime_config(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        workflow_code = str(payload.get("workflow_code") or "").strip()
        task_name = str(payload.get("task_name") or "").strip()
        task_code = self._safe_int(payload.get("task_code"))
        if not workflow_code:
            return False, {"message": "workflow_code is required"}
        if not task_name and task_code <= 0:
            return False, {"message": "task_name or task_code is required"}

        ok, workflow_result = self.get_workflow({"project_code": project_code, "workflow_code": workflow_code})
        if not ok:
            return False, workflow_result
        detail = self._unwrap_workflow_detail(workflow_result)
        task_definitions = self._get_workflow_task_definitions(detail)
        task = self._find_task(task_definitions, task_name=task_name, task_code=task_code)
        if not task:
            return False, {"message": "task not found", "task_name": task_name, "task_code": task_code}
        task_params = task.get("taskParams") or {}
        return True, {
            "project_code": project_code,
            "workflow_code": workflow_code,
            "task_code": self._safe_int(task.get("code")),
            "task_name": str(task.get("name") or "").strip(),
            "task_type": str(task.get("taskType") or "").strip(),
            "flag": str(task.get("flag") or "").strip(),
            "worker_group": task.get("workerGroup"),
            "environment_code": task.get("environmentCode"),
            "timeout": task.get("timeout"),
            "task_params": task_params,
            "runtime_config": {
                "datasource": task_params.get("datasource"),
                "sql_type": task_params.get("sqlType"),
                "local_params": task_params.get("localParams"),
                "resource_list": task_params.get("resourceList"),
                "raw_script": task_params.get("rawScript"),
                "sql": task_params.get("sql"),
                "tenant_code": detail.get("tenantCode") or self.config.tenant_code,
                "environment_code": task.get("environmentCode") or self.config.environment_code,
            },
        }

    def append_sql_task(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        payload = {**payload, "task_type": payload.get("task_type") or "SQL"}
        return self.append_task(payload)

    def append_shell_task(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        payload = {**payload, "task_type": payload.get("task_type") or "SHELL"}
        return self.append_task(payload)

    def update_sql_task(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        normalized_payload = {**payload, "task_type": payload.get("task_type") or "SQL"}
        task_params_patch = deepcopy(normalized_payload.get("task_params_patch") or {})
        if not isinstance(task_params_patch, dict):
            task_params_patch = {}
        sql_value = str(
            normalized_payload.get("sql")
            or normalized_payload.get("raw_sql")
            or normalized_payload.get("script")
            or ""
        ).strip()
        if sql_value:
            task_params_patch["sql"] = sql_value
        if task_params_patch:
            normalized_payload["task_params_patch"] = task_params_patch
        normalized_payload.pop("sql", None)
        normalized_payload.pop("raw_sql", None)
        normalized_payload.pop("script", None)
        payload = normalized_payload
        return self.update_task(payload)

    def update_shell_task(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        payload = {**payload, "task_type": payload.get("task_type") or "SHELL"}
        return self.update_task(payload)

    def update_task(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        workflow_code = str(payload.get("workflow_code") or "").strip()
        task_name = str(payload.get("task_name") or "").strip()
        task_code = self._safe_int(payload.get("task_code"))
        if not workflow_code:
            return False, {"message": "workflow_code is required"}
        if not task_name and task_code <= 0:
            return False, {"message": "task_name or task_code is required"}

        ok, workflow_result = self.request(
            "GET",
            f"/projects/{project_code}/workflow-definition/{workflow_code}",
        )
        if not ok:
            return False, workflow_result

        detail = self._unwrap_workflow_detail(workflow_result)
        if not detail:
            return False, {"message": "workflow detail payload is empty", "raw": workflow_result}

        task_definitions = self._get_workflow_task_definitions(detail)
        task_relations = self._get_workflow_task_relations(detail)
        locations = self._get_workflow_locations(detail)
        workflow_meta = self._get_workflow_meta(detail)
        integrity_issue = self._detect_workflow_param_integrity_issue(detail, task_definitions)
        if integrity_issue:
            return False, integrity_issue

        target_task = self._find_task(task_definitions, task_name=task_name, task_code=task_code)
        if not target_task:
            return False, {
                "message": "task not found",
                "task_name": task_name,
                "task_code": task_code if task_code > 0 else "",
            }

        mutated_task, change_summary = self._build_updated_task_definition(target_task, payload)
        if not change_summary.get("changed_fields"):
            return False, {
                "message": "nothing to update",
                "hint": "provide sql/script/local_params/resource_list/task_params_patch/task_description/datasource/sql_type/etc.",
                "task_name": str(target_task.get("name") or "").strip(),
                "task_code": self._safe_int(target_task.get("code")),
            }

        target_task_code = self._safe_int(target_task.get("code"))
        updated_task_definitions: list[Dict[str, Any]] = []
        for item in task_definitions:
            if self._safe_int(item.get("code")) == target_task_code:
                updated_task_definitions.append(mutated_task)
            else:
                updated_task_definitions.append(deepcopy(item))

        original_release_state = str(workflow_meta.get("releaseState") or "").upper()
        schedule_summary = self._resolve_schedule_summary(
            project_code=project_code,
            workflow_detail=detail,
        )
        original_schedule_release_state = str(schedule_summary.get("release_state") or "").upper()
        original_schedule_id = str(schedule_summary.get("schedule_id") or "").strip()
        restore_original_state = payload.get("restore_original_state", payload.get("restore_online", True))
        auto_offline = payload.get("auto_offline", True)
        was_online = original_release_state == "ONLINE"
        was_schedule_online = original_schedule_release_state == "ONLINE"

        if was_online and auto_offline:
            ok, offline_result = self.release_workflow(
                {"project_code": project_code, "workflow_code": workflow_code},
                "OFFLINE",
            )
            if not ok:
                return False, {
                    "message": "failed to offline workflow before update",
                    "detail": offline_result,
                }

        update_form = self._build_workflow_update_form(
            workflow_detail=detail,
            payload=payload,
            task_definitions=updated_task_definitions,
            task_relations=task_relations,
            locations=locations,
        )
        ok, update_result = self._update_workflow_definition(project_code, workflow_code, update_form)
        if not ok:
            return False, update_result
        if not self._is_ds_success(update_result):
            return False, {
                "message": "workflow update rejected by dolphinscheduler",
                "result": update_result,
            }

        restore_result = None
        if was_online and restore_original_state:
            restore_ok, restore_result = self.release_workflow(
                {"project_code": project_code, "workflow_code": workflow_code},
                "ONLINE",
            )
            if not restore_ok:
                return False, {
                    "message": "workflow updated but failed to restore original release state",
                    "update_result": update_result,
                    "restore_result": restore_result,
                    "original_release_state": original_release_state,
                }

        restore_schedule_result = None
        if was_schedule_online and restore_original_state and original_schedule_id:
            restore_schedule_ok, restore_schedule_result = self.release_schedule(
                {"project_code": project_code},
                original_schedule_id,
                "ONLINE",
            )
            if not restore_schedule_ok:
                return False, {
                    "message": "workflow updated but failed to restore original schedule release state",
                    "update_result": update_result,
                    "restore_result": restore_result,
                    "restore_schedule_result": restore_schedule_result,
                    "original_release_state": original_release_state,
                    "original_schedule_release_state": original_schedule_release_state,
                    "schedule_id": original_schedule_id,
                }

        return True, {
            "workflow_code": workflow_code,
            "project_code": project_code,
            "task_name": str(mutated_task.get("name") or "").strip(),
            "task_code": self._safe_int(mutated_task.get("code")),
            "task_type": str(mutated_task.get("taskType") or "").strip(),
            "change_summary": change_summary,
            "original_release_state": original_release_state,
            "original_schedule_release_state": original_schedule_release_state,
            "schedule_id": original_schedule_id,
            "schedule_summary": schedule_summary,
            "restored_original_state": bool(was_online and restore_original_state),
            "restored_original_schedule_state": bool(
                was_schedule_online and restore_original_state and original_schedule_id
            ),
            "update_result": update_result,
            "restore_result": restore_result,
            "restore_schedule_result": restore_schedule_result,
        }

    def disable_task(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        workflow_code = str(payload.get("workflow_code") or "").strip()
        task_name = str(payload.get("task_name") or "").strip()
        task_code = self._safe_int(payload.get("task_code"))
        if not workflow_code:
            return False, {"message": "workflow_code is required"}
        if not task_name and task_code <= 0:
            return False, {"message": "task_name or task_code is required"}

        ok, workflow_result = self.request(
            "GET",
            f"/projects/{project_code}/workflow-definition/{workflow_code}",
        )
        if not ok:
            return False, workflow_result

        detail = self._unwrap_workflow_detail(workflow_result)
        if not detail:
            return False, {"message": "workflow detail payload is empty", "raw": workflow_result}

        task_definitions = self._get_workflow_task_definitions(detail)
        task_relations = self._get_workflow_task_relations(detail)
        locations = self._get_workflow_locations(detail)
        workflow_meta = self._get_workflow_meta(detail)
        integrity_issue = self._detect_workflow_param_integrity_issue(detail, task_definitions)
        if integrity_issue:
            return False, integrity_issue

        target_task = self._find_task(task_definitions, task_name=task_name, task_code=task_code)
        if not target_task:
            return False, {
                "message": "task not found",
                "task_name": task_name,
                "task_code": task_code if task_code > 0 else "",
            }

        disable_task_code = self._safe_int(target_task.get("code"))
        disable_task_name = str(target_task.get("name") or "").strip()
        current_flag = str(target_task.get("flag") or "YES").strip().upper()

        if current_flag == "NO":
            return True, {
                "workflow_code": workflow_code,
                "project_code": project_code,
                "task_name": disable_task_name,
                "task_code": disable_task_code,
                "already_disabled": True,
            }

        updated_task_definitions: list[Dict[str, Any]] = []
        for item in task_definitions:
            cloned = deepcopy(item)
            if self._safe_int(cloned.get("code")) == disable_task_code:
                cloned["flag"] = "NO"
            updated_task_definitions.append(cloned)

        original_release_state = str(workflow_meta.get("releaseState") or "").upper()
        schedule_summary = self._resolve_schedule_summary(
            project_code=project_code,
            workflow_detail=detail,
        )
        original_schedule_release_state = str(schedule_summary.get("release_state") or "").upper()
        original_schedule_id = str(schedule_summary.get("schedule_id") or "").strip()
        restore_original_state = payload.get("restore_original_state", payload.get("restore_online", True))
        auto_offline = payload.get("auto_offline", True)
        was_online = original_release_state == "ONLINE"
        was_schedule_online = original_schedule_release_state == "ONLINE"

        if was_online and auto_offline:
            ok, offline_result = self.release_workflow(
                {"project_code": project_code, "workflow_code": workflow_code},
                "OFFLINE",
            )
            if not ok:
                return False, {
                    "message": "failed to offline workflow before update",
                    "detail": offline_result,
                }

        update_form = self._build_workflow_update_form(
            workflow_detail=detail,
            payload=payload,
            task_definitions=updated_task_definitions,
            task_relations=task_relations,
            locations=locations,
        )
        ok, update_result = self._update_workflow_definition(project_code, workflow_code, update_form)
        if not ok:
            return False, update_result
        if not self._is_ds_success(update_result):
            return False, {
                "message": "workflow update rejected by dolphinscheduler",
                "result": update_result,
            }

        restore_result = None
        if was_online and restore_original_state:
            restore_ok, restore_result = self.release_workflow(
                {"project_code": project_code, "workflow_code": workflow_code},
                "ONLINE",
            )
            if not restore_ok:
                return False, {
                    "message": "workflow updated but failed to restore original release state",
                    "update_result": update_result,
                    "restore_result": restore_result,
                    "original_release_state": original_release_state,
                }

        restore_schedule_result = None
        if was_schedule_online and restore_original_state and original_schedule_id:
            restore_schedule_ok, restore_schedule_result = self.release_schedule(
                {"project_code": project_code},
                original_schedule_id,
                "ONLINE",
            )
            if not restore_schedule_ok:
                return False, {
                    "message": "workflow updated but failed to restore original schedule release state",
                    "update_result": update_result,
                    "restore_result": restore_result,
                    "restore_schedule_result": restore_schedule_result,
                    "original_release_state": original_release_state,
                    "original_schedule_release_state": original_schedule_release_state,
                    "schedule_id": original_schedule_id,
                }

        return True, {
            "workflow_code": workflow_code,
            "project_code": project_code,
            "task_name": disable_task_name,
            "task_code": disable_task_code,
            "original_release_state": original_release_state,
            "original_schedule_release_state": original_schedule_release_state,
            "schedule_id": original_schedule_id,
            "schedule_summary": schedule_summary,
            "restored_original_state": bool(was_online and restore_original_state),
            "restored_original_schedule_state": bool(
                was_schedule_online and restore_original_state and original_schedule_id
            ),
            "update_result": update_result,
            "restore_result": restore_result,
            "restore_schedule_result": restore_schedule_result,
        }

    def disable_tasks_except(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        workflow_code = str(payload.get("workflow_code") or "").strip()
        if not workflow_code:
            return False, {"message": "workflow_code is required"}

        keep_task_names = {
            str(item).strip()
            for item in self._normalize_string_list(payload.get("keep_task_names"))
            if str(item).strip()
        }
        keep_task_codes = {
            self._safe_int(item)
            for item in self._normalize_list(payload.get("keep_task_codes"))
            if self._safe_int(item) > 0
        }
        if not keep_task_names and not keep_task_codes:
            return False, {"message": "keep_task_names or keep_task_codes is required"}

        target_task_name_prefixes = [
            str(item).strip()
            for item in self._normalize_string_list(
                payload.get("target_task_name_prefixes") or payload.get("task_name_prefixes")
            )
            if str(item).strip()
        ]

        ok, workflow_result = self.request(
            "GET",
            f"/projects/{project_code}/workflow-definition/{workflow_code}",
        )
        if not ok:
            return False, workflow_result

        detail = self._unwrap_workflow_detail(workflow_result)
        if not detail:
            return False, {"message": "workflow detail payload is empty", "raw": workflow_result}

        task_definitions = self._get_workflow_task_definitions(detail)
        task_relations = self._get_workflow_task_relations(detail)
        locations = self._get_workflow_locations(detail)
        workflow_meta = self._get_workflow_meta(detail)
        integrity_issue = self._detect_workflow_param_integrity_issue(detail, task_definitions)
        if integrity_issue:
            return False, integrity_issue

        updated_task_definitions: list[Dict[str, Any]] = []
        scoped_task_names: list[str] = []
        kept_task_names: list[str] = []
        disabled_task_names: list[str] = []
        already_disabled_task_names: list[str] = []

        for item in task_definitions:
            cloned = deepcopy(item)
            task_name = str(cloned.get("name") or "").strip()
            task_code = self._safe_int(cloned.get("code"))
            task_flag = str(cloned.get("flag") or "YES").strip().upper()

            in_scope = self._task_matches_disable_scope(
                task_name=task_name,
                target_task_name_prefixes=target_task_name_prefixes,
            )
            if not in_scope:
                updated_task_definitions.append(cloned)
                continue

            scoped_task_names.append(task_name)
            if task_name in keep_task_names or task_code in keep_task_codes:
                kept_task_names.append(task_name)
                updated_task_definitions.append(cloned)
                continue

            if task_flag == "NO":
                already_disabled_task_names.append(task_name)
                updated_task_definitions.append(cloned)
                continue

            cloned["flag"] = "NO"
            disabled_task_names.append(task_name)
            updated_task_definitions.append(cloned)

        if not scoped_task_names:
            return False, {
                "message": "no tasks matched disable scope",
                "target_task_name_prefixes": target_task_name_prefixes,
            }

        original_release_state = str(workflow_meta.get("releaseState") or "").upper()
        schedule_summary = self._resolve_schedule_summary(
            project_code=project_code,
            workflow_detail=detail,
        )
        original_schedule_release_state = str(schedule_summary.get("release_state") or "").upper()
        original_schedule_id = str(schedule_summary.get("schedule_id") or "").strip()
        restore_original_state = payload.get("restore_original_state", payload.get("restore_online", True))
        auto_offline = payload.get("auto_offline", True)
        was_online = original_release_state == "ONLINE"
        was_schedule_online = original_schedule_release_state == "ONLINE"

        if was_online and auto_offline:
            ok, offline_result = self.release_workflow(
                {"project_code": project_code, "workflow_code": workflow_code},
                "OFFLINE",
            )
            if not ok:
                return False, {
                    "message": "failed to offline workflow before update",
                    "detail": offline_result,
                }

        update_form = self._build_workflow_update_form(
            workflow_detail=detail,
            payload=payload,
            task_definitions=updated_task_definitions,
            task_relations=task_relations,
            locations=locations,
        )
        ok, update_result = self._update_workflow_definition(project_code, workflow_code, update_form)
        if not ok:
            return False, update_result
        if not self._is_ds_success(update_result):
            return False, {
                "message": "workflow update rejected by dolphinscheduler",
                "result": update_result,
            }

        restore_result = None
        if was_online and restore_original_state:
            restore_ok, restore_result = self.release_workflow(
                {"project_code": project_code, "workflow_code": workflow_code},
                "ONLINE",
            )
            if not restore_ok:
                return False, {
                    "message": "workflow updated but failed to restore original release state",
                    "update_result": update_result,
                    "restore_result": restore_result,
                    "original_release_state": original_release_state,
                }

        restore_schedule_result = None
        if was_schedule_online and restore_original_state and original_schedule_id:
            restore_schedule_ok, restore_schedule_result = self.release_schedule(
                {"project_code": project_code},
                original_schedule_id,
                "ONLINE",
            )
            if not restore_schedule_ok:
                return False, {
                    "message": "workflow updated but failed to restore original schedule release state",
                    "update_result": update_result,
                    "restore_result": restore_result,
                    "restore_schedule_result": restore_schedule_result,
                    "original_release_state": original_release_state,
                    "original_schedule_release_state": original_schedule_release_state,
                    "schedule_id": original_schedule_id,
                }

        return True, {
            "workflow_code": workflow_code,
            "project_code": project_code,
            "target_task_name_prefixes": target_task_name_prefixes,
            "keep_task_names": sorted(keep_task_names),
            "keep_task_codes": sorted(keep_task_codes),
            "scoped_task_count": len(scoped_task_names),
            "kept_task_count": len(kept_task_names),
            "disabled_task_count": len(disabled_task_names),
            "already_disabled_task_count": len(already_disabled_task_names),
            "scoped_task_names": scoped_task_names,
            "kept_task_names_matched": kept_task_names,
            "disabled_task_names": disabled_task_names,
            "already_disabled_task_names": already_disabled_task_names,
            "original_release_state": original_release_state,
            "original_schedule_release_state": original_schedule_release_state,
            "schedule_id": original_schedule_id,
            "schedule_summary": schedule_summary,
            "restored_original_state": bool(was_online and restore_original_state),
            "restored_original_schedule_state": bool(
                was_schedule_online and restore_original_state and original_schedule_id
            ),
            "update_result": update_result,
            "restore_result": restore_result,
            "restore_schedule_result": restore_schedule_result,
        }

    def delete_task(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        workflow_code = str(payload.get("workflow_code") or "").strip()
        task_name = str(payload.get("task_name") or "").strip()
        task_code = self._safe_int(payload.get("task_code"))
        if not workflow_code:
            return False, {"message": "workflow_code is required"}
        if not task_name and task_code <= 0:
            return False, {"message": "task_name or task_code is required"}

        ok, workflow_result = self.request(
            "GET",
            f"/projects/{project_code}/workflow-definition/{workflow_code}",
        )
        if not ok:
            return False, workflow_result

        detail = self._unwrap_workflow_detail(workflow_result)
        if not detail:
            return False, {"message": "workflow detail payload is empty", "raw": workflow_result}

        task_definitions = self._get_workflow_task_definitions(detail)
        task_relations = self._get_workflow_task_relations(detail)
        locations = self._get_workflow_locations(detail)
        workflow_meta = self._get_workflow_meta(detail)
        integrity_issue = self._detect_workflow_param_integrity_issue(detail, task_definitions)
        if integrity_issue:
            return False, integrity_issue

        target_task = self._find_task(task_definitions, task_name=task_name, task_code=task_code)
        if not target_task:
            return False, {
                "message": "task not found",
                "task_name": task_name,
                "task_code": task_code if task_code > 0 else "",
            }

        delete_task_code = self._safe_int(target_task.get("code"))
        delete_task_name = str(target_task.get("name") or "").strip()

        updated_task_definitions = [
            deepcopy(item)
            for item in task_definitions
            if self._safe_int(item.get("code")) != delete_task_code
        ]
        updated_locations = [
            deepcopy(item)
            for item in locations
            if self._safe_int(item.get("taskCode")) != delete_task_code
        ]
        updated_relations = self._remove_task_relations(
            task_relations=task_relations,
            task_definitions=task_definitions,
            delete_task_code=delete_task_code,
            project_code=project_code,
            workflow_code=workflow_code,
        )

        original_release_state = str(workflow_meta.get("releaseState") or "").upper()
        schedule_summary = self._resolve_schedule_summary(
            project_code=project_code,
            workflow_detail=detail,
        )
        original_schedule_release_state = str(schedule_summary.get("release_state") or "").upper()
        original_schedule_id = str(schedule_summary.get("schedule_id") or "").strip()
        restore_original_state = payload.get("restore_original_state", payload.get("restore_online", True))
        auto_offline = payload.get("auto_offline", True)
        was_online = original_release_state == "ONLINE"
        was_schedule_online = original_schedule_release_state == "ONLINE"

        if was_online and auto_offline:
            ok, offline_result = self.release_workflow(
                {"project_code": project_code, "workflow_code": workflow_code},
                "OFFLINE",
            )
            if not ok:
                return False, {
                    "message": "failed to offline workflow before update",
                    "detail": offline_result,
                }

        update_form = self._build_workflow_update_form(
            workflow_detail=detail,
            payload=payload,
            task_definitions=updated_task_definitions,
            task_relations=updated_relations,
            locations=updated_locations,
        )
        ok, update_result = self._update_workflow_definition(project_code, workflow_code, update_form)
        if not ok:
            return False, update_result
        if not self._is_ds_success(update_result):
            return False, {
                "message": "workflow update rejected by dolphinscheduler",
                "result": update_result,
            }

        restore_result = None
        if was_online and restore_original_state:
            restore_ok, restore_result = self.release_workflow(
                {"project_code": project_code, "workflow_code": workflow_code},
                "ONLINE",
            )
            if not restore_ok:
                return False, {
                    "message": "workflow updated but failed to restore original release state",
                    "update_result": update_result,
                    "restore_result": restore_result,
                    "original_release_state": original_release_state,
                }

        restore_schedule_result = None
        if was_schedule_online and restore_original_state and original_schedule_id:
            restore_schedule_ok, restore_schedule_result = self.release_schedule(
                {"project_code": project_code},
                original_schedule_id,
                "ONLINE",
            )
            if not restore_schedule_ok:
                return False, {
                    "message": "workflow updated but failed to restore original schedule release state",
                    "update_result": update_result,
                    "restore_result": restore_result,
                    "restore_schedule_result": restore_schedule_result,
                    "original_release_state": original_release_state,
                    "original_schedule_release_state": original_schedule_release_state,
                    "schedule_id": original_schedule_id,
                }

        return True, {
            "workflow_code": workflow_code,
            "project_code": project_code,
            "task_name": delete_task_name,
            "task_code": delete_task_code,
            "original_release_state": original_release_state,
            "original_schedule_release_state": original_schedule_release_state,
            "schedule_id": original_schedule_id,
            "schedule_summary": schedule_summary,
            "restored_original_state": bool(was_online and restore_original_state),
            "restored_original_schedule_state": bool(
                was_schedule_online and restore_original_state and original_schedule_id
            ),
            "update_result": update_result,
            "restore_result": restore_result,
            "restore_schedule_result": restore_schedule_result,
        }

    def append_task(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        workflow_code = str(payload.get("workflow_code") or "").strip()
        task_name = str(payload.get("task_name") or "").strip()
        task_type = self._normalize_task_type(payload.get("task_type") or "SQL")
        script_text = self._resolve_task_content(payload, task_type)
        if not workflow_code:
            return False, {"message": "workflow_code is required"}
        if not task_name:
            return False, {"message": "task_name is required"}
        if not script_text:
            content_label = "sql" if task_type == "SQL" else "script"
            return False, {"message": f"{content_label} is required"}

        ok, workflow_result = self.request(
            "GET",
            f"/projects/{project_code}/workflow-definition/{workflow_code}",
        )
        if not ok:
            return False, workflow_result

        detail = self._unwrap_workflow_detail(workflow_result)
        if not detail:
            return False, {"message": "workflow detail payload is empty", "raw": workflow_result}

        task_definitions = self._get_workflow_task_definitions(detail)
        task_relations = self._get_workflow_task_relations(detail)
        locations = self._get_workflow_locations(detail)
        workflow_meta = self._get_workflow_meta(detail)
        integrity_issue = self._detect_workflow_param_integrity_issue(detail, task_definitions)
        if integrity_issue:
            return False, integrity_issue
        if any(str(item.get("name", "")).strip() == task_name for item in task_definitions):
            return False, {"message": f"task already exists: {task_name}"}

        template_name = str(payload.get("template_task_name") or "").strip()
        template = self._find_template_task(task_definitions, template_name, task_type)
        if not template:
            for item in task_definitions:
                if isinstance(item, dict) and item:
                    template = deepcopy(item)
                    break
        if not template:
            return False, {
                "message": "workflow has no existing task nodes to clone from",
                "hint": "create_workflow bootstrap should create at least one starter task",
            }

        new_task_code = self._next_code(
            [self._safe_int(item.get("code")) for item in task_definitions]
        )
        new_task = self._build_task_from_template(
            template=template,
            task_name=task_name,
            task_code=new_task_code,
            task_type=task_type,
            script_text=script_text,
            payload=payload,
        )

        updated_task_definitions = deepcopy(task_definitions)
        updated_task_definitions.append(new_task)

        upstream_codes = self._resolve_upstream_codes(
            task_relations=task_relations,
            task_definitions=task_definitions,
            template_task=template,
            payload=payload,
        )
        if not upstream_codes:
            upstream_codes = [0]

        updated_locations = self._append_location(
            locations=locations,
            task_code=new_task_code,
            upstream_codes=upstream_codes,
        )
        updated_relations = self._append_relations(
            task_relations=task_relations,
            task_definitions=task_definitions,
            new_task=new_task,
            project_code=project_code,
            workflow_code=workflow_code,
            template_task=template,
            payload=payload,
            upstream_codes=upstream_codes,
        )

        original_release_state = str(workflow_meta.get("releaseState") or "").upper()
        schedule_summary = self._resolve_schedule_summary(
            project_code=project_code,
            workflow_detail=detail,
        )
        original_schedule_release_state = str(schedule_summary.get("release_state") or "").upper()
        original_schedule_id = str(schedule_summary.get("schedule_id") or "").strip()
        restore_original_state = payload.get("restore_original_state", payload.get("restore_online", True))
        auto_offline = payload.get("auto_offline", True)
        was_online = original_release_state == "ONLINE"
        was_schedule_online = original_schedule_release_state == "ONLINE"

        if was_online and auto_offline:
            ok, offline_result = self.release_workflow(
                {"project_code": project_code, "workflow_code": workflow_code},
                "OFFLINE",
            )
            if not ok:
                return False, {
                    "message": "failed to offline workflow before update",
                    "detail": offline_result,
                }

        update_form = self._build_workflow_update_form(
            workflow_detail=detail,
            payload=payload,
            task_definitions=updated_task_definitions,
            task_relations=updated_relations,
            locations=updated_locations,
        )
        ok, update_result = self._update_workflow_definition(project_code, workflow_code, update_form)
        if not ok:
            return False, update_result
        if not self._is_ds_success(update_result):
            return False, {
                "message": "workflow update rejected by dolphinscheduler",
                "result": update_result,
            }

        restore_result = None
        if was_online and restore_original_state:
            restore_ok, restore_result = self.release_workflow(
                {"project_code": project_code, "workflow_code": workflow_code},
                "ONLINE",
            )
            if not restore_ok:
                return False, {
                    "message": "workflow updated but failed to restore original release state",
                    "update_result": update_result,
                    "restore_result": restore_result,
                    "original_release_state": original_release_state,
                }

        restore_schedule_result = None
        if was_schedule_online and restore_original_state and original_schedule_id:
            restore_schedule_ok, restore_schedule_result = self.release_schedule(
                {"project_code": project_code},
                original_schedule_id,
                "ONLINE",
            )
            if not restore_schedule_ok:
                return False, {
                    "message": "workflow updated but failed to restore original schedule release state",
                    "update_result": update_result,
                    "restore_result": restore_result,
                    "restore_schedule_result": restore_schedule_result,
                    "original_release_state": original_release_state,
                    "original_schedule_release_state": original_schedule_release_state,
                    "schedule_id": original_schedule_id,
                }

        return True, {
            "workflow_code": workflow_code,
            "project_code": project_code,
            "task_name": task_name,
            "task_code": new_task_code,
            "task_type": task_type,
            "template_task_name": template.get("name"),
            "original_release_state": original_release_state,
            "original_schedule_release_state": original_schedule_release_state,
            "schedule_id": original_schedule_id,
            "schedule_summary": schedule_summary,
            "restored_original_state": bool(was_online and restore_original_state),
            "restored_original_schedule_state": bool(
                was_schedule_online and restore_original_state and original_schedule_id
            ),
            "update_result": update_result,
            "restore_result": restore_result,
            "restore_schedule_result": restore_schedule_result,
        }

    def _update_workflow_definition(
        self,
        project_code: str,
        workflow_code: str,
        form: Dict[str, Any],
    ) -> Tuple[bool, Any]:
        paths = [
            f"/projects/{project_code}/workflow-definition/{workflow_code}",
            f"/projects/{project_code}/workflow-definitions/{workflow_code}",
        ]
        attempts = []
        for path in paths:
            ok, result = self.request("PUT", path, form=form)
            if ok:
                return True, result
            attempts.append({"path": path, "result": result})
        return False, {"message": "all workflow update attempts failed", "attempts": attempts}

    def _create_workflow_definition(
        self,
        project_code: str,
        payload: Dict[str, Any],
    ) -> Tuple[bool, Any, Dict[str, Any]]:
        paths = [
            f"/projects/{project_code}/process-definition",
            f"/projects/{project_code}/process-definitions",
            f"/projects/{project_code}/workflow-definition",
            f"/projects/{project_code}/workflow-definitions",
        ]
        forms = self._build_workflow_create_forms(payload)
        attempts = []
        for path in paths:
            for form_label, form in forms:
                ok, result = self.request("POST", path, form=form)
                attempt = {
                    "path": path,
                    "form_label": form_label,
                    "form_keys": sorted(form.keys()),
                }
                if ok and self._is_ds_success(result):
                    return True, result, attempt
                attempt["result"] = result
                attempts.append(attempt)
        return False, {"message": "all workflow create attempts failed", "attempts": attempts}, {}

    def _build_workflow_create_forms(self, payload: Dict[str, Any]) -> list[tuple[str, Dict[str, Any]]]:
        workflow_name = str(payload.get("workflow_name") or "").strip()
        description = str(payload.get("description") or "").strip()
        global_params = self._normalize_json_value(payload.get("global_params"), default=[])
        timeout = payload.get("timeout")
        if timeout in ("", None):
            timeout = 0
        tenant_code = str(
            payload.get("tenant_code")
            or self.config.tenant_code
            or "default"
        ).strip()
        execution_type = str(payload.get("execution_type") or "PARALLEL").strip() or "PARALLEL"
        bootstrap_task_name = str(payload.get("bootstrap_task_name") or "__bootstrap_shell__").strip()
        bootstrap_script = str(
            payload.get("bootstrap_script")
            or payload.get("script")
            or "echo ds_scheduler_bootstrap_ok"
        ).strip()
        bootstrap_task_code = self._next_code([])
        bootstrap_relation_code = self._next_code([bootstrap_task_code])
        bootstrap_task = {
            "code": bootstrap_task_code,
            "version": 1,
            "name": bootstrap_task_name,
            "description": "bootstrap shell task created by ds-scheduler create_workflow",
            "taskType": "SHELL",
            "taskParams": {
                "localParams": [],
                "resourceList": [],
                "dependence": {},
                "conditionResult": {"successNode": [""], "failedNode": [""]},
                "waitStartTimeout": {},
                "switchResult": {},
                "rawScript": bootstrap_script,
            },
            "flag": "YES",
            "taskPriority": "MEDIUM",
            "workerGroup": self.config.worker_group,
            "environmentCode": payload.get("environment_code") or self.config.environment_code or "",
            "failRetryTimes": 0,
            "failRetryInterval": 1,
            "timeoutFlag": "CLOSE",
            "timeoutNotifyStrategy": "",
            "timeout": 0,
            "delayTime": 0,
            "taskGroupId": 0,
            "taskGroupPriority": 0,
            "cpuQuota": -1,
            "memoryMax": -1,
            "taskExecuteType": "BATCH",
            "projectCode": self._safe_int(payload.get("project_code") or self.config.project_code),
        }
        bootstrap_relation = {
            "name": "",
            "code": bootstrap_relation_code,
            "projectCode": self._safe_int(payload.get("project_code") or self.config.project_code),
            "processDefinitionCode": 0,
            "processDefinitionVersion": 1,
            "preTaskCode": 0,
            "preTaskVersion": 0,
            "postTaskCode": bootstrap_task_code,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": {},
        }
        bootstrap_locations = [{"taskCode": bootstrap_task_code, "x": 260, "y": 120}]
        full_form = {
            "name": workflow_name,
            "description": description,
            "globalParams": json.dumps(global_params, ensure_ascii=False),
            "locations": json.dumps([], ensure_ascii=False),
            "timeout": timeout,
            "tenantCode": tenant_code,
            "taskRelationJson": json.dumps([], ensure_ascii=False),
            "taskDefinitionJson": json.dumps([], ensure_ascii=False),
            "executionType": execution_type,
        }
        minimal_form = {
            "name": workflow_name,
            "description": description,
            "timeout": timeout,
            "tenantCode": tenant_code,
            "executionType": execution_type,
        }
        if global_params:
            minimal_form["globalParams"] = json.dumps(global_params, ensure_ascii=False)
        fallback_form = dict(minimal_form)
        fallback_form["globalParams"] = json.dumps(global_params, ensure_ascii=False)
        fallback_form["otherParamsJson"] = json.dumps({}, ensure_ascii=False)
        bootstrap_form = {
            "name": workflow_name,
            "description": description,
            "globalParams": json.dumps(global_params, ensure_ascii=False),
            "locations": json.dumps(bootstrap_locations, ensure_ascii=False),
            "timeout": timeout,
            "tenantCode": tenant_code,
            "taskRelationJson": json.dumps([bootstrap_relation], ensure_ascii=False),
            "taskDefinitionJson": json.dumps([bootstrap_task], ensure_ascii=False),
            "executionType": execution_type,
        }
        return [
            ("workflow_definition_bootstrap_shell", bootstrap_form),
            ("process_definition_minimal", minimal_form),
            ("process_definition_full_empty_graph", full_form),
            ("process_definition_other_params", fallback_form),
        ]

    def _build_workflow_update_form(
        self,
        workflow_detail: Dict[str, Any],
        payload: Dict[str, Any],
        task_definitions: list[Dict[str, Any]],
        task_relations: list[Dict[str, Any]],
        locations: list[Dict[str, Any]],
    ) -> Dict[str, Any]:
        workflow_meta = self._get_workflow_meta(workflow_detail)
        name = (
            payload.get("workflow_name")
            or workflow_meta.get("name")
            or ""
        )
        description = (
            payload.get("description")
            if payload.get("description") is not None
            else (
                workflow_meta.get("description")
                if workflow_meta.get("description") is not None
                else workflow_detail.get("description", "")
            )
        )
        global_params = self._normalize_json_value(
            workflow_meta.get("globalParams"),
            workflow_meta.get("globalParamList"),
            workflow_detail.get("globalParams"),
            workflow_detail.get("globalParamList"),
            default=[],
        )
        timeout = payload.get("timeout")
        if timeout in ("", None):
            timeout = workflow_meta.get("timeout") or 0
        tenant_code = (
            payload.get("tenant_code")
            or workflow_meta.get("tenantCode")
            or workflow_detail.get("tenantCode")
            or self.config.tenant_code
        )
        execution_type = (
            payload.get("execution_type")
            or workflow_meta.get("executionType")
            or "PARALLEL"
        )

        form = {
            "name": name,
            "description": description,
            "globalParams": json.dumps(global_params, ensure_ascii=False),
            "locations": json.dumps(locations, ensure_ascii=False),
            "timeout": timeout,
            "tenantCode": tenant_code,
            "taskRelationJson": json.dumps(task_relations, ensure_ascii=False),
            "taskDefinitionJson": json.dumps(task_definitions, ensure_ascii=False),
            "executionType": execution_type,
        }
        schedule_value = payload.get("schedule_json")
        if schedule_value is None:
            schedule_value = self._get_workflow_schedule(workflow_detail)
        if schedule_value not in ("", None, []):
            form["schedule"] = self._normalize_schedule_form_value(schedule_value)
        return form

    def _unwrap_workflow_detail(self, workflow_result: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(workflow_result, dict):
            return {}
        data = workflow_result.get("data")
        if isinstance(data, dict):
            return data
        return workflow_result

    def _get_workflow_meta(self, detail: Dict[str, Any]) -> Dict[str, Any]:
        workflow_meta = detail.get("workflowDefinition")
        if isinstance(workflow_meta, dict):
            return workflow_meta
        return detail

    def _get_workflow_task_definitions(self, detail: Dict[str, Any]) -> list[Dict[str, Any]]:
        direct = self._extract_json_list(
            detail,
            "taskDefinitionList",
            "taskDefinitionJson",
            "taskDefinitionJsonObj",
            "taskDefinitionJsonObject",
        )
        if direct:
            return direct
        nested = detail.get("workflowDefinition") or {}
        return self._extract_json_list(
            nested,
            "taskDefinitionList",
            "taskDefinitionJson",
            "taskDefinitionJsonObj",
            "taskDefinitionJsonObject",
        )

    def _get_workflow_task_relations(self, detail: Dict[str, Any]) -> list[Dict[str, Any]]:
        direct = self._extract_json_list(
            detail,
            "workflowTaskRelationList",
            "processTaskRelationList",
            "taskRelationJson",
            "taskRelationJsonObj",
            "taskRelationJsonObject",
        )
        if direct:
            return direct
        nested = detail.get("workflowDefinition") or {}
        return self._extract_json_list(
            nested,
            "workflowTaskRelationList",
            "processTaskRelationList",
            "taskRelationJson",
            "taskRelationJsonObj",
            "taskRelationJsonObject",
        )

    def _get_workflow_locations(self, detail: Dict[str, Any]) -> list[Dict[str, Any]]:
        direct = self._extract_json_list(
            detail,
            "locations",
            "locationsJson",
            "locationsObj",
            "locationsObject",
        )
        if direct:
            return direct
        nested = detail.get("workflowDefinition") or {}
        return self._extract_json_list(
            nested,
            "locations",
            "locationsJson",
            "locationsObj",
            "locationsObject",
        )

    def _detect_workflow_param_integrity_issue(
        self,
        workflow_detail: Dict[str, Any],
        task_definitions: list[Dict[str, Any]],
    ) -> Dict[str, Any] | None:
        workflow_global_names = self._extract_workflow_global_param_names(workflow_detail)
        if workflow_global_names:
            return None

        unresolved_by_task: list[Dict[str, Any]] = []
        for task in task_definitions:
            task_name = str(task.get("name") or "").strip()
            placeholders = self._extract_task_placeholders(task)
            if not placeholders:
                continue
            local_names = self._extract_task_local_param_names(task)
            unresolved = sorted(
                name
                for name in placeholders
                if name in self.RISKY_WORKFLOW_VARIABLES and name not in local_names
            )
            if unresolved:
                unresolved_by_task.append(
                    {
                        "task_name": task_name,
                        "task_code": self._safe_int(task.get("code")),
                        "missing_workflow_params": unresolved,
                    }
                )

        if not unresolved_by_task:
            return None

        workflow_meta = self._get_workflow_meta(workflow_detail)
        return {
            "message": "workflow global params are empty but tasks still reference required workflow variables",
            "workflow_name": workflow_meta.get("name"),
            "workflow_code": str(workflow_meta.get("code") or workflow_detail.get("code") or "").strip(),
            "required_workflow_params": sorted(
                {
                    name
                    for item in unresolved_by_task
                    for name in item["missing_workflow_params"]
                }
            ),
            "affected_tasks": unresolved_by_task,
            "hint": "restore workflow globalParams from t_ds_workflow_definition_log before mutating this workflow again",
        }

    def _extract_workflow_global_param_names(self, workflow_detail: Dict[str, Any]) -> set[str]:
        workflow_meta = self._get_workflow_meta(workflow_detail)
        names: set[str] = set()
        normalized = self._normalize_json_value(
            workflow_meta.get("globalParams"),
            workflow_meta.get("globalParamList"),
            workflow_detail.get("globalParams"),
            workflow_detail.get("globalParamList"),
            default=[],
        )
        if isinstance(normalized, list):
            for item in normalized:
                if isinstance(item, dict):
                    prop = str(item.get("prop") or "").strip()
                    if prop:
                        names.add(prop)
        for source in (workflow_meta.get("globalParamMap"), workflow_detail.get("globalParamMap")):
            if isinstance(source, dict):
                for key in source.keys():
                    prop = str(key or "").strip()
                    if prop:
                        names.add(prop)
        return names

    def _extract_task_local_param_names(self, task_definition: Dict[str, Any]) -> set[str]:
        names: set[str] = set()
        task_params = task_definition.get("taskParams") or {}
        if isinstance(task_params, dict):
            for item in task_params.get("localParams") or []:
                if isinstance(item, dict):
                    prop = str(item.get("prop") or "").strip()
                    if prop:
                        names.add(prop)
        for item in task_definition.get("taskParamList") or []:
            if isinstance(item, dict):
                prop = str(item.get("prop") or "").strip()
                if prop:
                    names.add(prop)
        for key in (task_definition.get("taskParamMap") or {}).keys():
            prop = str(key or "").strip()
            if prop:
                names.add(prop)
        return names

    def _extract_task_placeholders(self, task_definition: Dict[str, Any]) -> set[str]:
        task_params = task_definition.get("taskParams") or {}
        raw_script = ""
        if isinstance(task_params, dict):
            raw_script = str(task_params.get("rawScript") or task_params.get("sql") or "")
        return {
            str(match).strip()
            for match in re.findall(r"\$\{([^}]+)\}", raw_script)
            if str(match).strip()
        }

    def _find_task(
        self,
        task_definitions: Iterable[Dict[str, Any]],
        task_name: str = "",
        task_code: int = 0,
    ) -> Dict[str, Any] | None:
        normalized_name = str(task_name or "").strip()
        for item in task_definitions:
            if task_code > 0 and self._safe_int(item.get("code")) == task_code:
                return deepcopy(item)
            if normalized_name and str(item.get("name", "")).strip() == normalized_name:
                return deepcopy(item)
        return None

    def _task_matches_disable_scope(
        self,
        task_name: str,
        target_task_name_prefixes: list[str],
    ) -> bool:
        normalized_name = str(task_name or "").strip()
        if not target_task_name_prefixes:
            return True
        return any(normalized_name.startswith(prefix) for prefix in target_task_name_prefixes)

    def _normalize_list(self, raw: Any) -> list[Any]:
        if raw in (None, ""):
            return []
        if isinstance(raw, list):
            return raw
        return [raw]

    def _normalize_string_list(self, raw: Any) -> list[str]:
        return [str(item) for item in self._normalize_list(raw)]

    def _get_workflow_schedule(self, detail: Dict[str, Any]) -> Any:
        for source in (detail, detail.get("workflowDefinition") or {}):
            if not isinstance(source, dict):
                continue
            if source.get("schedule") not in (None, ""):
                return source.get("schedule")
        return None

    def _get_schedule_id(self, detail: Dict[str, Any]) -> str:
        schedule = self._get_workflow_schedule(detail)
        candidates: list[Any] = []
        if isinstance(schedule, dict):
            candidates.extend([schedule.get("id"), schedule.get("scheduleId")])
        elif isinstance(schedule, list):
            for item in schedule:
                if isinstance(item, dict):
                    candidates.extend([item.get("id"), item.get("scheduleId")])
        candidates.extend(
            [
                detail.get("scheduleId"),
                (detail.get("workflowDefinition") or {}).get("scheduleId"),
            ]
        )
        for candidate in candidates:
            value = str(candidate or "").strip()
            if value:
                return value
        return ""

    def _resolve_schedule_summary(
        self,
        project_code: str,
        workflow_detail: Dict[str, Any],
    ) -> Dict[str, Any]:
        workflow_meta = self._get_workflow_meta(workflow_detail)
        workflow_code = str(
            workflow_meta.get("code")
            or workflow_detail.get("code")
            or workflow_detail.get("workflowDefinitionCode")
            or ""
        ).strip()
        workflow_name = str(
            workflow_meta.get("name")
            or workflow_detail.get("name")
            or workflow_detail.get("workflowDefinitionName")
            or ""
        ).strip()

        # First try the workflow detail payload itself.
        detail_release_state = str(
            workflow_meta.get("scheduleReleaseState")
            or workflow_detail.get("scheduleReleaseState")
            or ""
        ).strip()
        detail_schedule_id = self._get_schedule_id(workflow_detail)
        if detail_release_state or detail_schedule_id:
            return {
                "found": bool(detail_release_state or detail_schedule_id),
                "source": "workflow_detail",
                "schedule_id": detail_schedule_id,
                "release_state": detail_release_state,
                "crontab": self._extract_schedule_crontab(self._get_workflow_schedule(workflow_detail)),
                "process_definition_code": workflow_code,
                "process_definition_name": workflow_name,
                "raw": self._get_workflow_schedule(workflow_detail),
            }

        # Fall back to the schedules list API, which is the real source of truth
        # for some DS 3.4 deployments.
        ok, result = self.request(
            "GET",
            f"/projects/{project_code}/schedules",
            query={"pageNo": 1, "pageSize": 200},
        )
        if not ok:
            return {
                "found": False,
                "source": "schedule_list_error",
                "schedule_id": "",
                "release_state": "",
                "crontab": "",
                "process_definition_code": workflow_code,
                "process_definition_name": workflow_name,
                "raw": result,
            }

        schedules = result.get("data", {}).get("totalList", [])
        if not isinstance(schedules, list):
            schedules = []

        matched = self._match_schedule_record(
            schedules=schedules,
            workflow_code=workflow_code,
            workflow_name=workflow_name,
        )
        if not matched:
            return {
                "found": False,
                "source": "schedule_list",
                "schedule_id": "",
                "release_state": "",
                "crontab": "",
                "process_definition_code": workflow_code,
                "process_definition_name": workflow_name,
                "raw": None,
            }

        return {
            "found": True,
            "source": "schedule_list",
            "schedule_id": str(matched.get("id") or matched.get("scheduleId") or "").strip(),
            "release_state": str(matched.get("releaseState") or matched.get("scheduleReleaseState") or "").strip(),
            "crontab": str(matched.get("crontab") or "").strip(),
            "process_definition_code": str(
                matched.get("processDefinitionCode")
                or matched.get("workflowDefinitionCode")
                or ""
            ).strip(),
            "process_definition_name": str(
                matched.get("processDefinitionName")
                or matched.get("workflowDefinitionName")
                or ""
            ).strip(),
            "raw": matched,
        }

    def _resolve_schedule_id(self, payload: Dict[str, Any]) -> str:
        schedule_id = str(payload.get("schedule_id") or "").strip()
        if schedule_id:
            return schedule_id
        workflow_code = str(payload.get("workflow_code") or "").strip()
        workflow_name = str(payload.get("workflow_name") or "").strip()
        if not workflow_code and not workflow_name:
            return ""
        ok, result = self.get_schedule(payload)
        if not ok or not isinstance(result, dict):
            return ""
        return str(result.get("id") or result.get("scheduleId") or "").strip()

    def _build_schedule_forms(
        self,
        payload: Dict[str, Any],
        *,
        project_code: str,
        workflow_code: str,
    ) -> list[tuple[str, Dict[str, Any]]]:
        schedule_value = payload.get("schedule_json")
        if schedule_value in ("", None):
            schedule_value = {
                "startTime": str(payload.get("start_time") or "").strip(),
                "endTime": str(payload.get("end_time") or "").strip(),
                "crontab": str(payload.get("crontab") or "").strip(),
                "timezoneId": str(payload.get("timezone_id") or payload.get("timezone") or "Asia/Shanghai").strip(),
            }

        process_instance_priority = str(
            payload.get("process_instance_priority")
            or payload.get("priority")
            or "MEDIUM"
        ).strip()

        base_form = {
            "warningType": str(payload.get("warning_type") or "NONE").strip(),
            "warningGroupId": str(payload.get("warning_group_id") or "0").strip(),
            "failureStrategy": str(payload.get("failure_strategy") or "CONTINUE").strip(),
            "processInstancePriority": process_instance_priority,
            "workerGroup": str(payload.get("worker_group") or self.config.worker_group or "default").strip(),
            "tenantCode": str(payload.get("tenant_code") or self.config.tenant_code or "default").strip(),
            "releaseState": str(payload.get("release_state") or "").strip(),
            "schedule": self._normalize_schedule_form_value(schedule_value),
        }
        environment_code = str(payload.get("environment_code") or self.config.environment_code or "").strip()
        if environment_code:
            base_form["environmentCode"] = environment_code
        custom_params = payload.get("custom_params") or {}
        if custom_params:
            base_form["startParams"] = json.dumps(custom_params, ensure_ascii=False)
        return [
            (
                "workflow_definition_code",
                {
                    **deepcopy(base_form),
                    "workflowDefinitionCode": workflow_code,
                },
            ),
            (
                "process_definition_code",
                {
                    **deepcopy(base_form),
                    "processDefinitionCode": workflow_code,
                },
            ),
        ]

    def _match_schedule_record(
        self,
        schedules: list[Dict[str, Any]],
        workflow_code: str,
        workflow_name: str,
    ) -> Dict[str, Any] | None:
        normalized_code = str(workflow_code or "").strip()
        normalized_name = str(workflow_name or "").strip()

        for item in schedules:
            process_code = str(
                item.get("processDefinitionCode")
                or item.get("workflowDefinitionCode")
                or ""
            ).strip()
            if normalized_code and process_code == normalized_code:
                return deepcopy(item)

        for item in schedules:
            process_name = str(
                item.get("processDefinitionName")
                or item.get("workflowDefinitionName")
                or ""
            ).strip()
            if normalized_name and process_name == normalized_name:
                return deepcopy(item)

        return None

    def _extract_schedule_crontab(self, schedule_value: Any) -> str:
        if isinstance(schedule_value, dict):
            return str(schedule_value.get("crontab") or "").strip()
        if isinstance(schedule_value, list):
            for item in schedule_value:
                if isinstance(item, dict):
                    value = str(item.get("crontab") or "").strip()
                    if value:
                        return value
        return ""

    def _extract_json_list(self, source: Dict[str, Any], *keys: str) -> list[Dict[str, Any]]:
        for key in keys:
            if key not in source:
                continue
            value = source.get(key)
            normalized = self._normalize_json_value(value, default=[])
            if isinstance(normalized, list):
                return [item for item in normalized if isinstance(item, dict)]
        return []

    def _normalize_json_value(self, *values: Any, default: Any = None) -> Any:
        for value in values:
            if value in ("", None):
                continue
            if isinstance(value, (list, dict)):
                return deepcopy(value)
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    continue
        return deepcopy(default)

    def _normalize_schedule_form_value(self, schedule_value: Any) -> str:
        if isinstance(schedule_value, str):
            return schedule_value
        return json.dumps(schedule_value, ensure_ascii=False)

    def _find_template_task(
        self,
        task_definitions: Iterable[Dict[str, Any]],
        template_name: str,
        task_type: str,
    ) -> Dict[str, Any] | None:
        typed_tasks = [
            item for item in task_definitions if str(item.get("taskType", "")).upper() == task_type
        ]
        if template_name:
            for item in typed_tasks:
                if str(item.get("name", "")).strip() == template_name:
                    return deepcopy(item)
        if typed_tasks:
            return deepcopy(typed_tasks[0])
        for item in task_definitions:
            if item:
                return deepcopy(item)
        return None

    def _build_task_from_template(
        self,
        template: Dict[str, Any],
        task_name: str,
        task_code: int,
        task_type: str,
        script_text: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        task = deepcopy(template)
        original_task_type = self._normalize_task_type(task.get("taskType") or "")
        if task_type == "SQL" and original_task_type != "SQL":
            sql_task = self._build_sql_task_definition(
                template=template,
                task_name=task_name,
                task_code=task_code,
                script_text=script_text,
                payload=payload,
            )
            return self._strip_task_server_fields(sql_task)
        if task_type == "SHELL" and original_task_type != "SHELL":
            shell_task = self._build_shell_task_definition(
                template=template,
                task_name=task_name,
                task_code=task_code,
                script_text=script_text,
                payload=payload,
            )
            return self._strip_task_server_fields(shell_task)
        task["code"] = task_code
        task["name"] = task_name
        task["taskType"] = task_type
        task["version"] = 1
        task["description"] = payload.get("task_description", task.get("description", ""))
        task.setdefault("flag", "YES")
        task.setdefault("taskPriority", "MEDIUM")
        task.setdefault("workerGroup", self.config.worker_group)
        if payload.get("environment_code") not in ("", None):
            task["environmentCode"] = payload["environment_code"]

        params = deepcopy(task.get("taskParams") or {})
        if not isinstance(params, dict):
            params = {}
        if task_type == "SQL" and original_task_type != "SQL":
            params = self._build_minimal_sql_task_params(script_text=script_text, payload=payload)
        params.setdefault("localParams", [])
        if task_type != "SQL":
            params.setdefault("resourceList", [])
            params.setdefault("dependence", {})
            params.setdefault("conditionResult", {"successNode": [""], "failedNode": [""]})
            params.setdefault("waitStartTimeout", {})
            params.setdefault("switchResult", {})
        if task_type == "SQL":
            datasource_meta = self._resolve_datasource_meta(payload)
            datasource_value = self._resolve_sql_task_datasource_value(
                payload, datasource_meta=datasource_meta
            )
            if datasource_meta:
                params["type"] = str(datasource_meta.get("type") or "").strip().upper()
            params.setdefault("sqlType", self._infer_sql_type(script_text))
            if payload.get("sql_type") not in ("", None):
                params["sqlType"] = self._normalize_sql_type(payload["sql_type"])
            if datasource_value not in ("", None):
                params["datasource"] = datasource_value
            params.setdefault("resourceList", [])
            params.setdefault("title", "")
            params.setdefault("receivers", "")
            params.setdefault("receiversCc", "")
            params.setdefault("showType", "TABLE")
            params.setdefault("connParams", "")
            params.setdefault("preStatements", [])
            params.setdefault("postStatements", [])
            if "displayRows" not in params:
                params["displayRows"] = 10
            sql_field = self._pick_first_existing_key(params, ["sql", "rawSql", "rawScript", "script"])
            params[sql_field] = script_text
            self._apply_sql_task_optional_fields(params, payload)
        else:
            script_field = self._pick_first_existing_key(params, ["rawScript", "script", "rawSql", "sql"])
            params[script_field] = script_text

        task["taskParams"] = self._apply_task_param_mutations(
            params,
            payload,
            default_local_params=params.get("localParams") or [],
        )
        return self._strip_task_server_fields(task)

    def _build_shell_task_definition(
        self,
        template: Dict[str, Any],
        task_name: str,
        task_code: int,
        script_text: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        template = deepcopy(template)
        base_task: Dict[str, Any] = {
            "code": task_code,
            "version": 1,
            "name": task_name,
            "description": payload.get("task_description", template.get("description", "")),
            "projectCode": self._safe_int(
                payload.get("project_code") or template.get("projectCode") or self.config.project_code
            ),
            "userId": self._safe_int(template.get("userId"), 0) or template.get("userId"),
            "taskType": "SHELL",
            "taskParamList": [],
            "taskParamMap": None,
            "flag": template.get("flag", "YES"),
            "taskPriority": template.get("taskPriority", "MEDIUM"),
            "workerGroup": template.get("workerGroup", self.config.worker_group),
            "environmentCode": (
                payload.get("environment_code")
                if payload.get("environment_code") not in ("", None)
                else template.get("environmentCode", self.config.environment_code or -1)
            ),
            "failRetryTimes": self._safe_int(template.get("failRetryTimes"), 0),
            "failRetryInterval": self._safe_int(template.get("failRetryInterval"), 1),
            "timeoutFlag": template.get("timeoutFlag", "CLOSE"),
            "timeoutNotifyStrategy": template.get("timeoutNotifyStrategy"),
            "timeout": self._safe_int(template.get("timeout"), 0),
            "delayTime": self._safe_int(template.get("delayTime"), 0),
            "resourceIds": template.get("resourceIds"),
            "taskGroupId": self._safe_int(template.get("taskGroupId"), 0),
            "taskGroupPriority": self._safe_int(template.get("taskGroupPriority"), 0),
            "cpuQuota": self._safe_int(template.get("cpuQuota"), -1),
            "memoryMax": self._safe_int(template.get("memoryMax"), -1),
            "taskExecuteType": template.get("taskExecuteType", "BATCH"),
            "taskParams": {
                "localParams": [],
                "resourceList": [],
                "dependence": {},
                "conditionResult": {"successNode": [""], "failedNode": [""]},
                "waitStartTimeout": {},
                "switchResult": {},
                "rawScript": script_text,
                "preStatements": [],
                "postStatements": [],
            },
        }
        base_task["taskParams"] = self._apply_task_param_mutations(
            base_task["taskParams"],
            payload,
            default_local_params=[],
        )
        return base_task

    def _build_sql_task_definition(
        self,
        template: Dict[str, Any],
        task_name: str,
        task_code: int,
        script_text: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        template = deepcopy(template)
        template_params = deepcopy(template.get("taskParams") or {})
        if not isinstance(template_params, dict):
            template_params = {}

        base_task: Dict[str, Any] = {
            "code": task_code,
            "version": 1,
            "name": task_name,
            "description": payload.get("task_description", template.get("description", "")),
            "projectCode": self._safe_int(
                payload.get("project_code") or template.get("projectCode") or self.config.project_code
            ),
            "userId": self._safe_int(template.get("userId"), 0) or template.get("userId"),
            "taskType": "SQL",
            "taskParamList": [],
            "taskParamMap": None,
            "flag": template.get("flag", "YES"),
            "taskPriority": template.get("taskPriority", "MEDIUM"),
            "workerGroup": template.get("workerGroup", self.config.worker_group),
            "environmentCode": (
                payload.get("environment_code")
                if payload.get("environment_code") not in ("", None)
                else template.get("environmentCode", self.config.environment_code or -1)
            ),
            "failRetryTimes": self._safe_int(template.get("failRetryTimes"), 0),
            "failRetryInterval": self._safe_int(template.get("failRetryInterval"), 1),
            "timeoutFlag": template.get("timeoutFlag", "CLOSE"),
            "timeoutNotifyStrategy": template.get("timeoutNotifyStrategy"),
            "timeout": self._safe_int(template.get("timeout"), 0),
            "delayTime": self._safe_int(template.get("delayTime"), 0),
            "resourceIds": template.get("resourceIds"),
            "taskGroupId": self._safe_int(template.get("taskGroupId"), 0),
            "taskGroupPriority": self._safe_int(template.get("taskGroupPriority"), 0),
            "cpuQuota": self._safe_int(template.get("cpuQuota"), -1),
            "memoryMax": self._safe_int(template.get("memoryMax"), -1),
            "taskExecuteType": template.get("taskExecuteType", "BATCH"),
        }

        params = self._build_minimal_sql_task_params(script_text=script_text, payload=payload)
        for key in (
            "resourceList",
            "title",
            "receivers",
            "receiversCc",
            "showType",
            "connParams",
            "preStatements",
            "postStatements",
            "displayRows",
            "localParams",
        ):
            if key in template_params and key not in params:
                params[key] = deepcopy(template_params[key])
        params = self._apply_task_param_mutations(
            params,
            payload,
            default_local_params=params.get("localParams") or [],
        )
        base_task["taskParams"] = params
        return base_task

    def _build_minimal_sql_task_params(self, script_text: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        datasource_meta = self._resolve_datasource_meta(payload)
        params: Dict[str, Any] = {
            "type": str(datasource_meta.get("type") or payload.get("datasource_type") or "").strip().upper(),
            "datasource": self._resolve_sql_task_datasource_value(
                payload, datasource_meta=datasource_meta
            ),
            "sql": script_text,
            "sqlType": self._normalize_sql_type(
                payload.get("sql_type")
                if payload.get("sql_type") not in ("", None)
                else self._infer_sql_type(script_text)
            ),
            "localParams": [],
            "resourceList": [],
            "title": "",
            "receivers": "",
            "receiversCc": "",
            "showType": "TABLE",
            "connParams": "",
            "preStatements": [],
            "postStatements": [],
            "displayRows": 10,
        }
        return params

    def _resolve_datasource_meta(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        datasource_id = payload.get("datasource_id") or payload.get("datasource")
        datasource_name = payload.get("datasource_name")
        if datasource_id in ("", None) and datasource_name in ("", None):
            return {}
        lookup_payload: Dict[str, Any] = {}
        if datasource_id not in ("", None):
            lookup_payload["datasource_id"] = datasource_id
        if datasource_name not in ("", None):
            lookup_payload["datasource"] = datasource_name
        ok, result = self.get_datasource(lookup_payload)
        if not ok:
            return {}
        if isinstance(result, dict):
            data = result.get("data")
            if isinstance(data, dict):
                return data
            return result
        return {}

    def _build_updated_task_definition(
        self,
        target_task: Dict[str, Any],
        payload: Dict[str, Any],
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        task = deepcopy(target_task)
        original_task_type = self._normalize_task_type(task.get("taskType") or "")
        original_params = deepcopy(task.get("taskParams") or {})
        task_type = self._normalize_task_type(payload.get("task_type") or task.get("taskType") or "SQL")
        task["taskType"] = task_type

        changed_fields: list[str] = []
        if payload.get("task_name") not in ("", None):
            new_name = str(payload.get("task_name") or "").strip()
            if new_name and new_name != str(task.get("name") or "").strip():
                task["name"] = new_name
                changed_fields.append("task_name")
        if payload.get("task_description") is not None:
            new_description = payload.get("task_description", "")
            if new_description != task.get("description", ""):
                task["description"] = new_description
                changed_fields.append("task_description")
        if payload.get("environment_code") not in ("", None):
            environment_code = str(payload.get("environment_code") or "").strip()
            if environment_code != str(task.get("environmentCode") or "").strip():
                task["environmentCode"] = environment_code
                changed_fields.append("environment_code")

        params = deepcopy(task.get("taskParams") or {})
        if not isinstance(params, dict):
            params = {}
        script_text = self._resolve_task_content(payload, task_type)
        if task_type == "SQL" and original_task_type != "SQL":
            rebuilt = self._build_sql_task_definition(
                template=task,
                task_name=str(task.get("name") or "").strip() or str(payload.get("task_name") or "").strip(),
                task_code=self._safe_int(task.get("code")),
                script_text=script_text or "select 1",
                payload=payload,
            )
            task.update({k: v for k, v in rebuilt.items() if k != "code"})
            params = deepcopy(rebuilt.get("taskParams") or {})
        params.setdefault("localParams", [])
        if task_type == "SQL":
            datasource_meta = self._resolve_datasource_meta(payload)
            datasource_value = self._resolve_sql_task_datasource_value(
                payload, datasource_meta=datasource_meta
            )
            if datasource_meta:
                params["type"] = str(datasource_meta.get("type") or "").strip().upper()
            params.setdefault("sqlType", self._infer_sql_type(script_text or str(params.get("sql") or "")))
            params.setdefault("resourceList", [])
            params.setdefault("title", "")
            params.setdefault("receivers", "")
            params.setdefault("receiversCc", "")
            params.setdefault("showType", "TABLE")
            params.setdefault("connParams", "")
            params.setdefault("preStatements", [])
            params.setdefault("postStatements", [])
            if "displayRows" not in params:
                params["displayRows"] = 10
        else:
            params.setdefault("resourceList", [])
            params.setdefault("dependence", {})
            params.setdefault("conditionResult", {"successNode": [""], "failedNode": [""]})
            params.setdefault("waitStartTimeout", {})
            params.setdefault("switchResult", {})

        if script_text:
            if task_type == "SQL":
                sql_field = self._pick_first_existing_key(params, ["sql", "rawSql", "rawScript", "script"])
                if str(params.get(sql_field) or "") != script_text:
                    params[sql_field] = script_text
                    changed_fields.append("sql")
                if payload.get("sql_type") not in ("", None):
                    sql_type = self._normalize_sql_type(payload["sql_type"])
                    if self._normalize_sql_type(params.get("sqlType")) != sql_type:
                        params["sqlType"] = sql_type
                        changed_fields.append("sql_type")
                self._apply_sql_task_optional_fields(params, payload)
            else:
                script_field = self._pick_first_existing_key(params, ["rawScript", "script", "rawSql", "sql"])
                if str(params.get(script_field) or "") != script_text:
                    params[script_field] = script_text
                    changed_fields.append("script")

        datasource_meta = self._resolve_datasource_meta(payload) if task_type == "SQL" else {}
        datasource_value = (
            self._resolve_sql_task_datasource_value(payload, datasource_meta=datasource_meta)
            if task_type == "SQL"
            else None
        )
        if datasource_value not in ("", None):
            if params.get("datasource") != datasource_value:
                params["datasource"] = datasource_value
                changed_fields.append("datasource")
            if datasource_meta:
                datasource_type = str(datasource_meta.get("type") or "").strip().upper()
                if datasource_type and params.get("type") != datasource_type:
                    params["type"] = datasource_type
                    changed_fields.append("datasource_type")

        params = self._apply_task_param_mutations(params, payload, default_local_params=params.get("localParams") or [])
        if json.dumps(params, ensure_ascii=False, sort_keys=True) != json.dumps(
            original_params, ensure_ascii=False, sort_keys=True
        ):
            changed_fields.append("task_params")

        task["taskParams"] = params
        return task, {
            "changed_fields": sorted(set(changed_fields)),
            "task_type": task_type,
        }

    def _apply_sql_task_optional_fields(self, params: Dict[str, Any], payload: Dict[str, Any]) -> None:
        field_mapping = {
            "title": "title",
            "receivers": "receivers",
            "receivers_cc": "receiversCc",
            "show_type": "showType",
            "conn_params": "connParams",
        }
        for payload_key, params_key in field_mapping.items():
            if payload_key not in payload:
                continue
            value = payload.get(payload_key)
            if value in ("", None):
                continue
            params[params_key] = value

    def _apply_task_param_mutations(
        self,
        params: Dict[str, Any],
        payload: Dict[str, Any],
        default_local_params: list[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        updated = deepcopy(params)
        raw_local_params = payload.get("local_params")
        if raw_local_params in (None, "", []):
            raw_local_params = payload.get("task_local_params")
        local_params = self._normalize_local_params(raw_local_params)
        if local_params:
            replace_local_params = bool(payload.get("replace_local_params", False))
            updated["localParams"] = self._merge_local_params(
                existing=updated.get("localParams") or default_local_params or [],
                incoming=local_params,
                replace=replace_local_params,
            )

        if "resource_list" in payload or "resources" in payload:
            resource_list = self._normalize_resource_list(
                payload.get("resource_list", payload.get("resources"))
            )
            replace_resource_list = bool(payload.get("replace_resource_list", True))
            updated["resourceList"] = self._merge_resource_list(
                existing=updated.get("resourceList") or [],
                incoming=resource_list,
                replace=replace_resource_list,
            )

        pre_statements = self._normalize_statement_list(payload.get("pre_statements"))
        if pre_statements is not None:
            updated["preStatements"] = pre_statements

        post_statements = self._normalize_statement_list(payload.get("post_statements"))
        if post_statements is not None:
            updated["postStatements"] = post_statements

        task_params_patch = payload.get("task_params_patch")
        if isinstance(task_params_patch, dict):
            for key, value in task_params_patch.items():
                updated[key] = deepcopy(value)

        return updated

    def _normalize_resource_list(self, raw: Any) -> list[Any]:
        if raw in (None, ""):
            return []
        if isinstance(raw, list):
            normalized: list[Any] = []
            for item in raw:
                if isinstance(item, dict):
                    normalized.append(deepcopy(item))
                    continue
                if isinstance(item, (int, float)):
                    normalized.append(int(item))
                    continue
                text = str(item).strip()
                if text:
                    normalized.append(text)
            return normalized
        if isinstance(raw, dict):
            return [deepcopy(raw)]
        text = str(raw).strip()
        return [text] if text else []

    def _normalize_local_params(self, raw: Any) -> list[Dict[str, Any]]:
        if raw in (None, "", []):
            return []
        if isinstance(raw, dict):
            normalized_list: list[Dict[str, Any]] = []
            for prop, value in raw.items():
                normalized_list.append(
                    {
                        "prop": str(prop),
                        "direct": "IN",
                        "type": self._infer_param_type(value),
                        "value": "" if value is None else str(value),
                    }
                )
            return normalized_list
        if not isinstance(raw, list):
            return []

        normalized: list[Dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            prop = str(item.get("prop") or "").strip()
            if not prop:
                continue
            direct = str(item.get("direct") or "IN").strip().upper() or "IN"
            param_type = str(item.get("type") or self._infer_param_type(item.get("value"))).strip().upper()
            value = item.get("value")
            normalized.append(
                {
                    "prop": prop,
                    "direct": direct,
                    "type": param_type or "VARCHAR",
                    "value": "" if value is None else str(value),
                }
            )
        return normalized

    def _merge_local_params(
        self,
        existing: list[Dict[str, Any]],
        incoming: list[Dict[str, Any]],
        *,
        replace: bool,
    ) -> list[Dict[str, Any]]:
        if replace:
            return deepcopy(incoming)

        merged_by_prop: dict[str, Dict[str, Any]] = {}
        order: list[str] = []
        for source in list(existing or []) + list(incoming or []):
            if not isinstance(source, dict):
                continue
            prop = str(source.get("prop") or "").strip()
            if not prop:
                continue
            if prop not in merged_by_prop:
                order.append(prop)
            merged_by_prop[prop] = deepcopy(source)
        return [merged_by_prop[prop] for prop in order]

    def _merge_resource_list(
        self,
        existing: list[Any],
        incoming: list[Any],
        *,
        replace: bool,
    ) -> list[Any]:
        if replace:
            return deepcopy(incoming)

        merged: list[Any] = []
        seen: set[str] = set()
        for source in list(existing or []) + list(incoming or []):
            key = json.dumps(source, ensure_ascii=False, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            merged.append(deepcopy(source))
        return merged

    def _normalize_statement_list(self, raw: Any) -> list[str] | None:
        if raw is None:
            return None
        if raw == "":
            return []
        if isinstance(raw, list):
            return [str(item) for item in raw if str(item).strip()]
        return [str(raw)]

    def _infer_param_type(self, value: Any) -> str:
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int):
            return "INTEGER"
        if isinstance(value, float):
            return "FLOAT"
        return "VARCHAR"

    def _append_location(
        self,
        locations: list[Dict[str, Any]],
        task_code: int,
        upstream_codes: list[int] | None = None,
    ) -> list[Dict[str, Any]]:
        updated = deepcopy(locations)
        upstream_codes = upstream_codes or []
        location_by_code = {
            self._safe_int(item.get("taskCode")): item
            for item in updated
            if self._safe_int(item.get("taskCode")) > 0
        }

        anchor = None
        for upstream_code in upstream_codes:
            if upstream_code > 0 and upstream_code in location_by_code:
                anchor = location_by_code[upstream_code]
                break

        if anchor:
            anchor_x = self._safe_int(anchor.get("x"))
            anchor_y = self._safe_int(anchor.get("y"), 120)
            sibling_count = sum(
                1
                for item in updated
                if self._safe_int(item.get("x")) > anchor_x
                and abs(self._safe_int(item.get("y")) - anchor_y) <= 220
            )
            new_x = anchor_x + 260
            new_y = anchor_y + (sibling_count * 120)
        else:
            last_x = 0
            last_y = 120
            for item in updated:
                last_x = max(last_x, self._safe_int(item.get("x")))
                last_y = max(last_y, self._safe_int(item.get("y")))
            new_x = last_x + 260
            new_y = last_y + 120

        updated.append({"taskCode": task_code, "x": new_x, "y": new_y})
        return updated

    def _append_relations(
        self,
        task_relations: list[Dict[str, Any]],
        task_definitions: list[Dict[str, Any]],
        new_task: Dict[str, Any],
        project_code: str,
        workflow_code: str,
        template_task: Dict[str, Any],
        payload: Dict[str, Any],
        upstream_codes: list[int] | None = None,
    ) -> list[Dict[str, Any]]:
        updated = deepcopy(task_relations)
        existing_codes = [self._safe_int(item.get("code")) for item in updated]
        new_task_code = self._safe_int(new_task.get("code"))

        upstream_codes = upstream_codes or self._resolve_upstream_codes(
            task_relations=task_relations,
            task_definitions=task_definitions,
            template_task=template_task,
            payload=payload,
        )
        if not upstream_codes:
            upstream_codes = [0]
        skeleton = deepcopy(updated[0]) if updated else {}
        workflow_version = self._resolve_workflow_relation_version(task_relations)
        for upstream_code in upstream_codes:
            relation = deepcopy(skeleton)
            relation["name"] = ""
            relation["projectCode"] = self._safe_int(project_code)
            relation["processDefinitionCode"] = self._safe_int(workflow_code)
            relation["workflowDefinitionCode"] = self._safe_int(workflow_code)
            relation["processDefinitionVersion"] = workflow_version
            relation["workflowDefinitionVersion"] = workflow_version
            relation["preTaskCode"] = upstream_code
            relation["preTaskVersion"] = self._find_task_version(task_definitions, upstream_code)
            relation["postTaskCode"] = new_task_code
            relation["postTaskVersion"] = self._safe_int(new_task.get("version"), 1)
            relation["conditionType"] = self._normalize_relation_condition_type(
                relation.get("conditionType")
            )
            relation["conditionParams"] = relation.get("conditionParams", {})
            relation.pop("id", None)
            relation_code = self._next_code(existing_codes)
            relation["code"] = relation_code
            existing_codes.append(relation_code)
            updated.append(relation)
        return updated

    def _remove_task_relations(
        self,
        task_relations: list[Dict[str, Any]],
        task_definitions: list[Dict[str, Any]],
        delete_task_code: int,
        project_code: str,
        workflow_code: str,
    ) -> list[Dict[str, Any]]:
        predecessors: list[int] = []
        successors: list[int] = []
        preserved_relations: list[Dict[str, Any]] = []

        for relation in task_relations:
            pre_code = self._safe_int(relation.get("preTaskCode"))
            post_code = self._safe_int(relation.get("postTaskCode"))
            if post_code == delete_task_code:
                predecessors.append(pre_code)
                continue
            if pre_code == delete_task_code:
                successors.append(post_code)
                continue
            preserved_relations.append(deepcopy(relation))

        predecessors = self._dedupe_ints(predecessors)
        successors = self._dedupe_ints(successors)
        existing_codes = [self._safe_int(item.get("code")) for item in preserved_relations]
        existing_pairs = {
            (self._safe_int(item.get("preTaskCode")), self._safe_int(item.get("postTaskCode")))
            for item in preserved_relations
        }

        skeleton = deepcopy(task_relations[0]) if task_relations else {}
        workflow_version = self._resolve_workflow_relation_version(task_relations)
        for predecessor in predecessors:
            for successor in successors:
                if predecessor == successor:
                    continue
                pair = (predecessor, successor)
                if pair in existing_pairs:
                    continue
                relation = deepcopy(skeleton)
                relation["name"] = ""
                relation["projectCode"] = self._safe_int(project_code)
                relation["processDefinitionCode"] = self._safe_int(workflow_code)
                relation["workflowDefinitionCode"] = self._safe_int(workflow_code)
                relation["processDefinitionVersion"] = workflow_version
                relation["workflowDefinitionVersion"] = workflow_version
                relation["preTaskCode"] = predecessor
                relation["preTaskVersion"] = 1 if predecessor == 0 else self._find_task_version(task_definitions, predecessor)
                relation["postTaskCode"] = successor
                relation["postTaskVersion"] = self._find_task_version(task_definitions, successor)
                relation["conditionType"] = self._normalize_relation_condition_type(
                    relation.get("conditionType")
                )
                relation["conditionParams"] = relation.get("conditionParams", {})
                relation.pop("id", None)
                relation_code = self._next_code(existing_codes)
                relation["code"] = relation_code
                existing_codes.append(relation_code)
                existing_pairs.add(pair)
                preserved_relations.append(relation)

        return preserved_relations

    def _resolve_upstream_codes(
        self,
        task_relations: list[Dict[str, Any]],
        task_definitions: list[Dict[str, Any]],
        template_task: Dict[str, Any],
        payload: Dict[str, Any],
    ) -> list[int]:
        explicit_code = self._safe_int(payload.get("upstream_task_code"))
        if explicit_code > 0:
            return [explicit_code]

        explicit_name = str(payload.get("upstream_task_name") or "").strip()
        if explicit_name:
            for item in task_definitions:
                if str(item.get("name", "")).strip() == explicit_name:
                    return [self._safe_int(item.get("code"))]

        template_code = self._safe_int(template_task.get("code"))
        if template_code > 0:
            return [template_code]

        return self._leaf_task_codes(task_relations, task_definitions)

    def _leaf_task_codes(
        self,
        task_relations: list[Dict[str, Any]],
        task_definitions: list[Dict[str, Any]],
    ) -> list[int]:
        task_codes = {
            self._safe_int(item.get("code"))
            for item in task_definitions
            if self._safe_int(item.get("code")) > 0
        }
        upstream = set()
        downstream = set()
        for relation in task_relations:
            pre_code = self._safe_int(relation.get("preTaskCode"))
            post_code = self._safe_int(relation.get("postTaskCode"))
            if pre_code > 0:
                upstream.add(pre_code)
            if post_code > 0:
                downstream.add(post_code)
        if not task_codes:
            return []
        leafs = sorted(code for code in task_codes if code not in upstream)
        if leafs:
            return leafs
        return sorted(task_codes)

    def _find_task_version(self, task_definitions: list[Dict[str, Any]], task_code: int) -> int:
        for item in task_definitions:
            if self._safe_int(item.get("code")) == task_code:
                return self._safe_int(item.get("version"), 1)
        return 1

    def _pick_first_existing_key(self, source: Dict[str, Any], keys: list[str]) -> str:
        for key in keys:
            if key in source:
                return key
        return keys[0]

    def _strip_task_server_fields(self, task: Dict[str, Any]) -> Dict[str, Any]:
        cleaned = deepcopy(task)
        for key in (
            "id",
            "createTime",
            "updateTime",
            "modifyBy",
            "userName",
            "projectName",
            "operator",
            "operateTime",
        ):
            cleaned.pop(key, None)
        return cleaned

    def _resolve_workflow_relation_version(self, task_relations: list[Dict[str, Any]]) -> int:
        for item in task_relations:
            for key in ("workflowDefinitionVersion", "processDefinitionVersion"):
                version = self._safe_int(item.get(key), 0)
                if version > 0:
                    return version
        return 1

    def _normalize_relation_condition_type(self, value: Any) -> Any:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
        return "NONE"

    def _infer_sql_type(self, sql_text: str) -> str:
        prefix = sql_text.lstrip().lower()
        for keyword in ("select", "with", "show", "desc", "explain"):
            if prefix.startswith(keyword):
                return "query"
        return "non_query"

    def _normalize_sql_type(self, sql_type: Any) -> str:
        if isinstance(sql_type, str):
            normalized = sql_type.strip().lower()
            aliases = {
                "0": "query",
                "query": "query",
                "select": "query",
                "read": "query",
                "查询": "query",
                "1": "non_query",
                "non_query": "non_query",
                "non-query": "non_query",
                "nonquery": "non_query",
                "update": "non_query",
                "write": "non_query",
                "execute": "non_query",
                "非查询": "non_query",
            }
            if normalized in aliases:
                return aliases[normalized]
        normalized_int = self._safe_int(sql_type, -1)
        if normalized_int in (0, 1):
            return "query" if normalized_int == 0 else "non_query"
        return "non_query"

    def _resolve_sql_task_datasource_value(
        self,
        payload: Dict[str, Any],
        *,
        datasource_meta: Dict[str, Any] | None = None,
    ) -> Any:
        datasource_meta = datasource_meta or {}
        for key in ("id", "datasourceId"):
            value = datasource_meta.get(key)
            if value not in ("", None):
                return self._safe_int(value, value)

        datasource_id = payload.get("datasource_id")
        if datasource_id not in ("", None):
            return self._safe_int(datasource_id, datasource_id)

        datasource = payload.get("datasource")
        if isinstance(datasource, (int, float)):
            return int(datasource)
        datasource_text = str(datasource or "").strip()
        if datasource_text.isdigit():
            return int(datasource_text)
        return datasource

    def _normalize_task_type(self, task_type: Any) -> str:
        normalized = str(task_type or "SQL").strip().upper()
        aliases = {
            "SQL": "SQL",
            "SHELL": "SHELL",
            "COMMAND": "SHELL",
            "SCRIPT": "SHELL",
        }
        return aliases.get(normalized, normalized)

    def _resolve_task_content(self, payload: Dict[str, Any], task_type: str) -> str:
        if task_type == "SQL":
            return str(payload.get("sql") or payload.get("raw_sql") or payload.get("script") or "").strip()
        return str(
            payload.get("script")
            or payload.get("raw_script")
            or payload.get("shell")
            or payload.get("command")
            or ""
        ).strip()

    def _safe_int(self, value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _dedupe_ints(self, values: Iterable[int]) -> list[int]:
        seen: set[int] = set()
        result: list[int] = []
        for value in values:
            normalized = self._safe_int(value)
            if normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
        return result

    def _next_code(self, existing_codes: Iterable[int]) -> int:
        current_max = max((code for code in existing_codes if code > 0), default=0)
        candidate = max(current_max + 1, int(datetime.now().timestamp() * 1000))
        candidate += random.randint(100, 999)
        return candidate

    def _is_ds_success(self, result: Any) -> bool:
        if not isinstance(result, dict):
            return False
        raw_value = result.get("raw")
        if isinstance(raw_value, str) and self._looks_like_html_document(raw_value):
            return False
        if result.get("success") is False:
            return False
        if result.get("failed") is True:
            return False
        code = result.get("code")
        if code not in (None, 0, "0"):
            return False
        return True

    def _extract_workflow_code(self, result: Any) -> str:
        if not isinstance(result, dict):
            return ""
        candidates: list[Any] = [
            result.get("code"),
            result.get("workflowCode"),
            result.get("workflowDefinitionCode"),
        ]
        data = result.get("data")
        if isinstance(data, dict):
            candidates.extend(
                [
                    data.get("code"),
                    data.get("workflowCode"),
                    data.get("workflowDefinitionCode"),
                    data.get("processDefinitionCode"),
                ]
            )
        for candidate in candidates:
            value = str(candidate or "").strip()
            if value and value != "0":
                return value
        return ""
