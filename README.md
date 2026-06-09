# ds-scheduler-gateway

面向 Codex + n8n 的多国家 DolphinScheduler 3.4 调度操作网关。

这个项目的目标是把 6 个国家的 DS 调度增删改查能力，从各国旧项目中抽离出来，统一成一套可维护的标准入口。

## 设计目标

- 一套代码支持多个国家
- 国家差异只放在配置层
- n8n 只负责接收请求、按国家分流、跳板机执行
- 目标机器只需要拉取这个项目，并提供本国 DS 网络访问能力
- Codex 通过统一 webhook 请求调用调度能力

## 第一版支持动作

- `list_workflows`
- `get_workflow`
- `online_workflow`
- `offline_workflow`
- `trigger_workflow`
- `list_instances`
- `get_instance`
- `append_sql_task`
- `dump_workflow_graph`

## 目录结构

```text
.
├── README.md
├── requirements.txt
├── config/
│   ├── countries.example.json
│   └── countries.json
├── gateway/
│   ├── __init__.py
│   ├── main.py
│   ├── models.py
│   ├── response.py
│   ├── router.py
│   └── utils.py
├── clients/
│   ├── __init__.py
│   └── dolphinscheduler_client.py
├── handlers/
│   ├── __init__.py
│   └── workflow_handlers.py
└── scripts/
    └── ds_scheduler_entry.py
```

## 请求模型

项目入口脚本接收这些参数：

- `--country`
- `--action`
- `--ds-token`
- `--request-id`
- `--payload-b64`

其中 `payload-b64` 是 webhook 中 `payload` 的 base64 编码，避免命令行转义问题。

## 配置说明

默认读取：

```text
config/countries.json
```

可以通过环境变量覆盖：

```bash
export DS_COUNTRIES_CONFIG=/root/ds-scheduler-gateway/config/countries.json
```

`countries.json` 当前已经预置了 6 个国家的基础配置：

- `cn`
- `ine`
- `mx`
- `ph`
- `pk`
- `th`

其中 `trigger_workflow` 依赖的 `environment_code` / `tenant_code` / `worker_group` /
`start_endpoint` / `start_code_field` 现在支持多层覆盖，优先级如下：

1. 请求 payload 显式传入
2. `workflow_overrides[workflow_code]`
3. `project_overrides[project_code]`
4. `action_overrides[action]`
5. 国家默认配置

这样做的目的是兼容“同一个国家里，不同工作流或不同触发方式使用不同运行参数”的情况，
同时不影响你现有的 n8n 调用方式。

## 当前内置国家配置来源

这些值来自各国现有 `Intelligent-Alarm-Repair-Assistant` 仓库中的运行配置：

- `cn`: `DS_BASE_URL=http://172.20.0.235:12345/dolphinscheduler`
- `ine`: `DS_BASE_URL=http://192.168.21.236:12345/dolphinscheduler`
- `mx`: `DS_BASE_URL=http://172.20.220.165:12345/dolphinscheduler`
- `ph`: `DS_BASE_URL=http://127.0.0.1:12345/dolphinscheduler`
- `pk`: `DS_BASE_URL=http://10.20.84.176:12345/dolphinscheduler`
- `th`: `DS_BASE_URL=http://192.168.20.236:12345/dolphinscheduler`

说明：

- `ph` 的 DS 地址是 `127.0.0.1`，意味着网关脚本需要部署在菲律宾目标机本机执行
- `pk` 的 `trigger_workflow` 使用 `start-workflow-instance` + `workflowDefinitionCode`
- 其余国家当前按 DS 3.4 标准 `start-process-instance` + `processDefinitionCode` 处理

## 本地示例

```bash
cd /root/ds-scheduler-gateway && python3 scripts/ds_scheduler_entry.py \
  --country cn \
  --action list_workflows \
  --ds-token 'your_token' \
  --request-id 'req-001' \
  --payload-b64 'eyJwYWdlX25vIjoxLCJwYWdlX3NpemUiOjIwfQ=='
```

## n8n 中国节点示例

```bash
ssh -p 36000 root@10.20.47.14 "cd /root/ds-scheduler-gateway && python3 scripts/ds_scheduler_entry.py --country cn --action '{{$json.action}}' --ds-token '{{$json.ds_token}}' --request-id '{{$json.request_id}}' --payload-b64 '{{$json.payload_b64}}'"
```

## n8n 多国家命令模板

把下面命令中的 SSH 主机替换成各国真实目标机即可：

```bash
ssh -p <port> root@<country-host> "cd /root/ds-scheduler-gateway && python3 scripts/ds_scheduler_entry.py --country <country> --action '{{$json.action}}' --ds-token '{{$json.ds_token}}' --request-id '{{$json.request_id}}' --payload-b64 '{{$json.payload_b64}}'"
```

示例：

- 中国：

```bash
ssh -p 36000 root@10.20.47.14 "cd /root/ds-scheduler-gateway && python3 scripts/ds_scheduler_entry.py --country cn --action '{{$json.action}}' --ds-token '{{$json.ds_token}}' --request-id '{{$json.request_id}}' --payload-b64 '{{$json.payload_b64}}'"
```

- 泰国：

```bash
ssh -p 36000 root@192.168.20.236 "cd /root/ds-scheduler-gateway && python3 scripts/ds_scheduler_entry.py --country th --action '{{$json.action}}' --ds-token '{{$json.ds_token}}' --request-id '{{$json.request_id}}' --payload-b64 '{{$json.payload_b64}}'"
```

- 巴基斯坦：

```bash
ssh -p 36000 root@<pk-host> "cd /root/ds-scheduler-gateway && python3 scripts/ds_scheduler_entry.py --country pk --action '{{$json.action}}' --ds-token '{{$json.ds_token}}' --request-id '{{$json.request_id}}' --payload-b64 '{{$json.payload_b64}}'"
```

## 落地建议

第一版建议：

1. 先把 `scaffold` 部署到 6 个国家实际执行机
2. 按国家校验 `countries.json` 中的 `environment_code` / `tenant_code`
3. 在 n8n 中把每个国家分支接到对应跳板机命令
4. 逐个国家先测 `list_workflows`
5. 再统一回归测试 `trigger_workflow`
6. 后续再扩展：
   - 创建工作流
   - 更新工作流定义
   - 上下线 schedule
   - 停止实例
   - 查询任务节点详情

## 新增 SQL 任务

`append_sql_task` 会先读取当前工作流定义，然后：

1. 自动寻找当前工作流中的一个 SQL 节点作为模板
2. 继承它的 datasource / tenant / worker / environment 等运行参数
3. 追加一个新的 SQL 任务节点
4. 默认把新节点挂到模板节点后面；也支持显式指定上游节点
5. 更新完成后会恢复到工作流原来的发布状态

推荐 payload：

```json
{
  "project_code": "19427088052704",
  "workflow_code": "174599383687393",
  "task_name": "测试2",
  "sql": "select 2",
  "template_task_name": "dwd_okr_dashboard"
}
```

说明：

- `task_name`: 新任务名，必填
- `sql`: 新 SQL 文本，必填
- `template_task_name`: 可选。建议显式指定一个现有 SQL 节点名，避免模板选择错误
- `upstream_task_name`: 可选。显式指定把新节点挂到哪个已有节点后面
- `upstream_task_code`: 可选。优先级高于 `upstream_task_name`
- `restore_original_state`: 可选，默认 `true`
- `auto_offline`: 可选，默认 `true`
- `sql_type`: 可选。查询通常传 `0`，非查询传 `1`

示例 base64 原文：

```json
{"project_code":"19427088052704","workflow_code":"174599383687393","task_name":"测试2","sql":"select 2","template_task_name":"dwd_okr_dashboard"}
```

## 导出工作流图结构

`dump_workflow_graph` 用来排查工作流 DAG 结构问题，返回：

1. `workflow_summary`
2. `task_definitions`
3. `task_relations`
4. `locations`
5. `raw_workflow_detail`

推荐 payload：

```json
{
  "project_code": "19427088052704",
  "workflow_code": "174599383687393"
}
```
