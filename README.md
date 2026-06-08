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

## 落地建议

第一版建议：

1. 先在中国机器部署并跑通
2. 补齐 `countries.json`
3. 在 n8n 中把每个国家分支接到对应跳板机命令
4. 后续再扩展：
   - 创建工作流
   - 更新工作流定义
   - 上下线 schedule
   - 停止实例
   - 查询任务节点详情
