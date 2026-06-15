from __future__ import annotations

import json
import random
from copy import deepcopy
from datetime import datetime
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, Tuple

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
        return self.request(
            "GET",
            f"/projects/{project_code}/schedules",
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
            json_body={
                "processInstanceId": instance_id,
                "executeType": execute_type,
            },
        )
        if not ok:
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
        path = f"/projects/{project_code}/schedules/{schedule_id}/release"
        attempts = []
        for method in ("POST", "PUT"):
            ok, result = self.request(
                method,
                path,
                query={"releaseState": release_state},
            )
            if ok:
                return True, result
            attempts.append({"method": method, "result": result})
            status = result.get("status") if isinstance(result, dict) else None
            if status not in (405, 404):
                break
        return False, {
            "message": "all schedule release attempts failed",
            "path": path,
            "attempts": attempts,
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

    def append_sql_task(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        payload = {**payload, "task_type": payload.get("task_type") or "SQL"}
        return self.append_task(payload)

    def append_shell_task(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        payload = {**payload, "task_type": payload.get("task_type") or "SHELL"}
        return self.append_task(payload)

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
        if any(str(item.get("name", "")).strip() == task_name for item in task_definitions):
            return False, {"message": f"task already exists: {task_name}"}

        template_name = str(payload.get("template_task_name") or "").strip()
        template = self._find_template_task(task_definitions, template_name, task_type)
        if not template:
            return False, {
                "message": f"no {task_type} task template found in workflow",
                "hint": f"set payload.template_task_name to an existing {task_type} task name in this workflow",
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

    def _build_workflow_update_form(
        self,
        workflow_detail: Dict[str, Any],
        payload: Dict[str, Any],
        task_definitions: list[Dict[str, Any]],
        task_relations: list[Dict[str, Any]],
        locations: list[Dict[str, Any]],
    ) -> Dict[str, Any]:
        name = (
            payload.get("workflow_name")
            or self._get_workflow_meta(workflow_detail).get("name")
            or ""
        )
        description = (
            payload.get("description")
            if payload.get("description") is not None
            else workflow_detail.get("description", "")
        )
        global_params = self._normalize_json_value(
            workflow_detail.get("globalParams"),
            workflow_detail.get("globalParamList"),
            default=[],
        )
        timeout = payload.get("timeout")
        if timeout in ("", None):
            timeout = (
                self._get_workflow_meta(workflow_detail).get("timeout")
                or 0
            )
        tenant_code = (
            payload.get("tenant_code")
            or workflow_detail.get("tenantCode")
            or self.config.tenant_code
        )
        execution_type = (
            payload.get("execution_type")
            or self._get_workflow_meta(workflow_detail).get("executionType")
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
        params.setdefault("localParams", [])
        params.setdefault("resourceList", [])
        params.setdefault("dependence", {})
        params.setdefault("conditionResult", {"successNode": [""], "failedNode": [""]})
        params.setdefault("waitStartTimeout", {})
        params.setdefault("switchResult", {})
        if task_type == "SQL":
            params.setdefault("sqlType", self._infer_sql_type(script_text))
            if payload.get("sql_type") is not None:
                params["sqlType"] = self._normalize_sql_type(payload["sql_type"])
            if payload.get("datasource") not in ("", None):
                params["datasource"] = payload["datasource"]
            sql_field = self._pick_first_existing_key(params, ["sql", "rawSql", "rawScript", "script"])
            params[sql_field] = script_text
        else:
            script_field = self._pick_first_existing_key(params, ["rawScript", "script", "rawSql", "sql"])
            params[script_field] = script_text

        task["taskParams"] = params
        return task

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
        for upstream_code in upstream_codes:
            relation = deepcopy(skeleton)
            relation["name"] = ""
            relation["projectCode"] = self._safe_int(project_code)
            relation["processDefinitionCode"] = self._safe_int(workflow_code)
            relation["preTaskCode"] = upstream_code
            relation["preTaskVersion"] = self._find_task_version(task_definitions, upstream_code)
            relation["postTaskCode"] = new_task_code
            relation["postTaskVersion"] = self._safe_int(new_task.get("version"), 1)
            relation["conditionType"] = relation.get("conditionType", 0)
            relation["conditionParams"] = relation.get("conditionParams", {})
            relation["code"] = self._next_code(existing_codes)
            existing_codes.append(relation["code"])
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
                relation["preTaskCode"] = predecessor
                relation["preTaskVersion"] = 1 if predecessor == 0 else self._find_task_version(task_definitions, predecessor)
                relation["postTaskCode"] = successor
                relation["postTaskVersion"] = self._find_task_version(task_definitions, successor)
                relation["conditionType"] = relation.get("conditionType", 0)
                relation["conditionParams"] = relation.get("conditionParams", {})
                relation["code"] = self._next_code(existing_codes)
                existing_codes.append(relation["code"])
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

    def _infer_sql_type(self, sql_text: str) -> int:
        prefix = sql_text.lstrip().lower()
        for keyword in ("select", "with", "show", "desc", "explain"):
            if prefix.startswith(keyword):
                return 0
        return 1

    def _normalize_sql_type(self, sql_type: Any) -> int:
        if isinstance(sql_type, str):
            normalized = sql_type.strip().lower()
            aliases = {
                "0": 0,
                "query": 0,
                "select": 0,
                "read": 0,
                "1": 1,
                "non_query": 1,
                "non-query": 1,
                "update": 1,
                "write": 1,
                "execute": 1,
            }
            if normalized in aliases:
                return aliases[normalized]
        return self._safe_int(sql_type)

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
        if result.get("success") is False:
            return False
        if result.get("failed") is True:
            return False
        code = result.get("code")
        if code not in (None, 0, "0"):
            return False
        return True
