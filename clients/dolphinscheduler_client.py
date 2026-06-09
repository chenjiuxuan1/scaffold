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

    def append_sql_task(self, payload: Dict[str, Any]) -> Tuple[bool, Any]:
        project_code = str(payload.get("project_code") or self.config.project_code).strip()
        workflow_code = str(payload.get("workflow_code") or "").strip()
        task_name = str(payload.get("task_name") or "").strip()
        sql_text = str(payload.get("sql") or payload.get("raw_sql") or "").strip()
        if not workflow_code:
            return False, {"message": "workflow_code is required"}
        if not task_name:
            return False, {"message": "task_name is required"}
        if not sql_text:
            return False, {"message": "sql is required"}

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
        template = self._find_template_task(task_definitions, template_name)
        if not template:
            return False, {
                "message": "no SQL task template found in workflow",
                "hint": "set payload.template_task_name to an existing SQL task name in this workflow",
            }

        new_task_code = self._next_code(
            [self._safe_int(item.get("code")) for item in task_definitions]
        )
        new_task = self._build_sql_task_from_template(
            template=template,
            task_name=task_name,
            task_code=new_task_code,
            sql_text=sql_text,
            payload=payload,
        )

        updated_task_definitions = deepcopy(task_definitions)
        updated_task_definitions.append(new_task)

        updated_locations = self._append_location(
            locations=locations,
            task_code=new_task_code,
        )
        updated_relations = self._append_relations(
            task_relations=task_relations,
            task_definitions=task_definitions,
            new_task=new_task,
            project_code=project_code,
            workflow_code=workflow_code,
            template_task=template,
            payload=payload,
        )

        original_release_state = str(
            workflow_meta.get("releaseState")
            or workflow_meta.get("scheduleReleaseState")
            or workflow_meta.get("release_state")
            or ""
        ).upper()
        restore_original_state = payload.get("restore_original_state", payload.get("restore_online", True))
        auto_offline = payload.get("auto_offline", True)
        was_online = original_release_state == "ONLINE"

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

        return True, {
            "workflow_code": workflow_code,
            "project_code": project_code,
            "task_name": task_name,
            "task_code": new_task_code,
            "template_task_name": template.get("name"),
            "original_release_state": original_release_state,
            "restored_original_state": bool(was_online and restore_original_state),
            "update_result": update_result,
            "restore_result": restore_result,
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
        if payload.get("schedule_json") is not None:
            form["schedule"] = payload["schedule_json"]
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

    def _find_template_task(
        self,
        task_definitions: Iterable[Dict[str, Any]],
        template_name: str,
    ) -> Dict[str, Any] | None:
        sql_tasks = [
            item for item in task_definitions if str(item.get("taskType", "")).upper() == "SQL"
        ]
        if template_name:
            for item in sql_tasks:
                if str(item.get("name", "")).strip() == template_name:
                    return deepcopy(item)
        if sql_tasks:
            return deepcopy(sql_tasks[0])
        for item in task_definitions:
            if item:
                return deepcopy(item)
        return None

    def _build_sql_task_from_template(
        self,
        template: Dict[str, Any],
        task_name: str,
        task_code: int,
        sql_text: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        task = deepcopy(template)
        task["code"] = task_code
        task["name"] = task_name
        task["taskType"] = "SQL"
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
        params.setdefault("sqlType", self._infer_sql_type(sql_text))
        if payload.get("sql_type") is not None:
            params["sqlType"] = payload["sql_type"]
        if payload.get("datasource") not in ("", None):
            params["datasource"] = payload["datasource"]

        sql_field = self._pick_first_existing_key(params, ["sql", "rawSql", "rawScript", "script"])
        params[sql_field] = sql_text

        task["taskParams"] = params
        return task

    def _append_location(
        self,
        locations: list[Dict[str, Any]],
        task_code: int,
    ) -> list[Dict[str, Any]]:
        updated = deepcopy(locations)
        last_x = 0
        last_y = 120
        for item in updated:
            last_x = max(last_x, self._safe_int(item.get("x")))
            last_y = max(last_y, self._safe_int(item.get("y")))
        updated.append({"taskCode": task_code, "x": last_x + 220, "y": last_y})
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
    ) -> list[Dict[str, Any]]:
        updated = deepcopy(task_relations)
        existing_codes = [self._safe_int(item.get("code")) for item in updated]
        new_task_code = self._safe_int(new_task.get("code"))

        upstream_codes = self._resolve_upstream_codes(
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

    def _safe_int(self, value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

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
