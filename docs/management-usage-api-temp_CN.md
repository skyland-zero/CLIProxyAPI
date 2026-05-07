# CLIProxyAPI 统计接口文档

本文档整理了 CLIProxyAPI 当前可用的统计相关 Management API，覆盖统计开关、API Key 使用情况、OpenAI 定价、Usage 聚合、Usage 明细、Usage 汇总和 Usage 队列读取接口。

## 1. 公共说明

- 基础前缀：`/v0/management`
- 认证方式：
  - `Authorization: Bearer <MANAGEMENT_KEY>`
  - `X-Management-Key: <MANAGEMENT_KEY>`
- 管理接口默认仅允许本机访问；远程访问还需要开启 `remote-management.allow-remote`
- 内建统计数据保留期：`3` 天
- 时间参数支持：
  - `RFC3339/RFC3339Nano`，例如 `2026-05-07T10:30:00Z`
  - Unix 秒时间戳，例如 `1746613800`

常见错误响应：

```json
{"error":"missing management key"}
```

```json
{"error":"invalid management key"}
```

```json
{"error":"remote management disabled"}
```

## 2. 统计开关

### 2.1 获取统计开关

`GET /v0/management/usage-statistics-enabled`

返回示例：

```json
{
  "usage-statistics-enabled": true
}
```

### 2.2 更新统计开关

`PUT /v0/management/usage-statistics-enabled`

`PATCH /v0/management/usage-statistics-enabled`

请求体：

```json
{
  "value": true
}
```

成功响应：

```json
{
  "status": "ok"
}
```

失败响应：

```json
{
  "error": "invalid body"
}
```

## 3. API Key 请求统计

### 3.1 获取 API Key 使用情况

`GET /v0/management/api-key-usage`

说明：

- 返回当前内存中的 `api_key` 类型账号请求统计
- 第一层按 `provider` 分组
- 第二层 key 为 `base_url|api_key`
- `recent_requests` 为最近 `20` 个时间桶，每桶 `10` 分钟，时间格式类似 `15:00-15:10`

返回示例：

```json
{
  "codex": {
    "https://codex.example.com|codex-key": {
      "success": 12,
      "failed": 2,
      "recent_requests": [
        {"time": "14:00-14:10", "success": 0, "failed": 0},
        {"time": "14:10-14:20", "success": 1, "failed": 0}
      ]
    }
  },
  "claude": {
    "https://claude.example.com|claude-key": {
      "success": 5,
      "failed": 0,
      "recent_requests": [
        {"time": "14:00-14:10", "success": 1, "failed": 0}
      ]
    }
  }
}
```

## 4. 定价接口

### 4.1 获取 OpenAI 定价缓存

`GET /v0/management/pricing/openai`

返回示例：

```json
{
  "mode": "memory",
  "provider": "openai",
  "prices": [
    {
      "provider": "openai",
      "model": "gpt-5",
      "category": "standard",
      "context": "short_context",
      "modality": "text",
      "unit": "1m_tokens",
      "input_per_1m": 1.25,
      "cached_input_per_1m": 0.125,
      "output_per_1m": 10,
      "training_per_hour": 0,
      "price_per_second": 0,
      "source_url": "https://developers.openai.com/api/docs/pricing",
      "fetched_at": "2026-05-07T10:00:00Z",
      "updated_at": "2026-05-07T10:00:00Z"
    }
  ]
}
```

### 4.2 刷新 OpenAI 定价

`POST /v0/management/pricing/openai/refresh`

成功响应：

```json
{
  "mode": "memory",
  "provider": "openai",
  "count": 24,
  "prices": [
    {
      "provider": "openai",
      "model": "gpt-5"
    }
  ]
}
```

失败响应：

```json
{
  "error": "failed to refresh pricing: upstream unavailable"
}
```

说明：

- 该接口会主动从上游定价页面抓取并刷新本地缓存
- 上游失败时返回 `502 Bad Gateway`

## 5. Usage 聚合统计

### 5.1 获取 Usage 快照

`GET /v0/management/usage`

公共查询参数：

| 参数 | 类型 | 说明 |
|---|---|---|
| `provider` | string | 按 provider 过滤 |
| `model` | string | 按 model 过滤 |
| `alias` | string | 按 alias 过滤 |
| `auth_id` | string | 按 auth_id 过滤 |
| `auth_type` | string | 按 auth_type 过滤 |
| `source` | string | 按 source 过滤 |
| `from` | string | 起始时间 |
| `to` | string | 结束时间 |
| `api_key_hash` | string | 直接按 hash 过滤 |
| `api_key` | string | 明文 key，服务端会转成 hash |
| `failed` | bool | 仅看成功或失败请求 |

返回示例：

```json
{
  "mode": "memory",
  "retention_days": 3,
  "failed_requests": 2,
  "usage": {
    "total_requests": 10,
    "success_count": 8,
    "failure_count": 2,
    "total_tokens": 152340,
    "apis": {
      "sha256:xxxx": {
        "total_requests": 10,
        "total_tokens": 152340,
        "models": {
          "gpt-5": {
            "total_requests": 6,
            "total_tokens": 102340,
            "details": [
              {
                "timestamp": "2026-05-07T10:00:00Z",
                "latency_ms": 820,
                "source": "openai",
                "auth_index": "auth-1",
                "auth_id": "abc",
                "auth_type": "api_key",
                "request_id": "req_123",
                "endpoint": "/v1/responses",
                "api_key_hash": "sha256:xxxx",
                "tokens": {
                  "input_tokens": 1000,
                  "output_tokens": 500,
                  "reasoning_tokens": 0,
                  "cached_tokens": 0,
                  "total_tokens": 1500
                },
                "failed": false
              }
            ]
          }
        }
      }
    },
    "requests_by_day": {"2026-05-07": 10},
    "requests_by_hour": {"10": 4},
    "tokens_by_day": {"2026-05-07": 152340},
    "tokens_by_hour": {"10": 64000}
  }
}
```

## 6. Usage 明细事件

### 6.1 获取 Usage 事件列表

`GET /v0/management/usage/events`

查询参数：

- 支持第 5.1 节全部公共参数
- `limit`：正整数，默认 `100`，最大按服务端规则截断到 `1000`
- `offset`：非负整数，默认 `0`

返回示例：

```json
{
  "mode": "memory",
  "retention_days": 3,
  "total": 1,
  "limit": 10,
  "offset": 0,
  "events": [
    {
      "id": "event-1",
      "timestamp": "2026-05-07T10:00:00Z",
      "request_id": "req_123",
      "endpoint": "/v1/responses",
      "provider": "openai",
      "model": "gpt",
      "alias": "gpt",
      "auth_id": "auth-1",
      "auth_index": "idx-1",
      "auth_type": "api_key",
      "source": "openai",
      "api_key_hash": "sha256:xxxx",
      "latency_ms": 780,
      "failed": false,
      "tokens": {
        "input_tokens": 1000000,
        "output_tokens": 1000000,
        "reasoning_tokens": 0,
        "cached_tokens": 0,
        "total_tokens": 2000000
      },
      "estimated_cost_usd": 3
    }
  ]
}
```

说明：

- 该接口是非破坏性读取
- `estimated_cost_usd` 可能为 `null`，表示当前没有匹配到定价

### 6.2 删除 Usage 事件

`DELETE /v0/management/usage/events`

查询参数：

- 支持第 5.1 节全部公共参数
- 额外支持 `before`：删除该时间点之前的数据

请求示例：

```http
DELETE /v0/management/usage/events?provider=openai&before=2026-05-07T00:00:00Z
```

成功响应：

```json
{
  "mode": "memory",
  "retention_days": 3,
  "deleted": 128
}
```

失败响应：

```json
{
  "error": "invalid before"
}
```

## 7. Usage 汇总

### 7.1 获取 Usage Summary

`GET /v0/management/usage/summary`

查询参数：

- 支持第 5.1 节全部公共参数
- `group_by` 可选值：
  - `provider`
  - `model`
  - `alias`
  - `auth_id`
  - `auth_type`
  - `source`
  - `day`
  - `hour`
  - `api_key_hash`
- 默认值：`provider`

返回示例：

```json
{
  "mode": "memory",
  "retention_days": 3,
  "group_by": "provider",
  "summary": [
    {
      "group": "openai",
      "total_requests": 2,
      "success_count": 1,
      "failure_count": 1,
      "input_tokens": 1000,
      "output_tokens": 200,
      "reasoning_tokens": 0,
      "cached_tokens": 0,
      "total_tokens": 1200,
      "estimated_cost_usd": 0.0034,
      "priced_requests": 2,
      "unpriced_requests": 0
    }
  ]
}
```

说明：

- 返回的是聚合后结果，不返回单条事件
- 汇总成本来自逐条事件估算后再累加

## 8. Usage 队列

### 8.1 获取 Usage 队列内容

`GET /v0/management/usage-queue`

查询参数：

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `count` | int | `1` | 读取并弹出多少条队列记录 |

返回示例：

```json
[
  {
    "timestamp": "2026-05-07T10:00:00Z",
    "latency_ms": 820,
    "source": "openai",
    "auth_index": "idx-1",
    "tokens": {
      "input_tokens": 1000,
      "output_tokens": 500,
      "reasoning_tokens": 0,
      "cached_tokens": 0,
      "total_tokens": 1500
    },
    "failed": false,
    "provider": "openai",
    "model": "gpt-5",
    "alias": "gpt-5",
    "endpoint": "/v1/responses",
    "auth_type": "api_key",
    "api_key": "sk-xxx",
    "request_id": "req_123"
  }
]
```

失败响应：

```json
{
  "error": "count must be a positive integer"
}
```

重要说明：

- 该接口是破坏性读取，读取后数据会从队列中移除
- 当前服务已启用后台消费者持续把队列数据落盘时，这个接口不适合作为外部稳定采集入口
- 如果要做长期采集，优先使用持久化后的 `/usage`、`/usage/events`、`/usage/summary`

## 9. 调用示例

```bash
curl -H "Authorization: Bearer <MANAGEMENT_KEY>" \
  "http://127.0.0.1:8080/v0/management/usage/events?provider=openai&limit=20"
```

```bash
curl -X POST -H "Authorization: Bearer <MANAGEMENT_KEY>" \
  "http://127.0.0.1:8080/v0/management/pricing/openai/refresh"
```

```bash
curl -X PATCH -H "Authorization: Bearer <MANAGEMENT_KEY>" \
  -H "Content-Type: application/json" \
  -d "{\"value\":true}" \
  "http://127.0.0.1:8080/v0/management/usage-statistics-enabled"
```
