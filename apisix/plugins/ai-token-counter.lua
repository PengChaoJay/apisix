--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
local core = require("apisix.core")
local plugin = require("apisix.plugin")
local base_prometheus = require("prometheus")
local ngx = ngx

local plugin_name = "ai-token-counter"

local DEFAULT_BUCKETS = {1, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000}
local DEFAULT_COST_BUCKETS = {0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0}

local schema = {
    type = "object",
    properties = {
        limit = {
            type = "object",
            properties = {
                prompt_tokens = {
                    type = "integer",
                    minimum = 0,
                    description = "Max prompt tokens per minute",
                },
                completion_tokens = {
                    type = "integer",
                    minimum = 0,
                    description = "Max completion tokens per minute",
                },
                total_tokens = {
                    type = "integer",
                    minimum = 0,
                    description = "Max total tokens per minute",
                },
            },
        },
        pricing = {
            type = "object",
            description = "Token pricing per 1M tokens",
            properties = {
                prompt_tokens = {
                    type = "number",
                    minimum = 0,
                    default = 0,
                    description = "Price per 1M prompt tokens",
                },
                completion_tokens = {
                    type = "number",
                    minimum = 0,
                    default = 0,
                    description = "Price per 1M completion tokens",
                },
            },
        },
        redis = {
            type = "object",
            properties = {
                host = {
                    type = "string",
                    default = "127.0.0.1"
                },
                port = {
                    type = "integer",
                    default = 6379
                },
                password = {
                    type = "string",
                },
                database = {
                    type = "integer",
                    default = 0
                },
                key_prefix = {
                    type = "string",
                    default = "ai_token:"
                },
            },
        },
        log = {
            type = "object",
            properties = {
                enabled = {
                    type = "boolean",
                    default = true,
                },
                response_headers = {
                    type = "boolean",
                    default = false,
                    description = "Add token info to response headers",
                },
                prometheus = {
                    type = "boolean",
                    default = true,
                    description = "Export Prometheus metrics",
                },
            },
        },
    },
}

local metadata_schema = {
    type = "object",
    properties = {
        id = {
            type = "string",
        },
        metric_prefix = {
            type = "string",
            default = "ai_token_",
            description = "Prefix for Prometheus metric names",
        },
    },
    required = {"id"},
}

local _M = {
    version = 0.2,
    priority = 1005,
    name = plugin_name,
    schema = schema,
    metadata_schema = metadata_schema,
}

local prometheus
local metrics = {}
local redis_client

local function init_prometheus()
    local attr = plugin.plugin_attr(plugin_name)
    local metric_prefix = "ai_token_"

    if attr and attr.metric_prefix then
        metric_prefix = attr.metric_prefix
    end

    prometheus = base_prometheus.init("prometheus-metrics", metric_prefix)

    metrics.prompt_tokens = prometheus:counter("prompt_tokens_total",
        "Total number of prompt tokens processed",
        {"provider", "model", "consumer", "route"})

    metrics.completion_tokens = prometheus:counter("completion_tokens_total",
        "Total number of completion tokens generated",
        {"provider", "model", "consumer", "route"})

    metrics.total_tokens = prometheus:counter("total_tokens_total",
        "Total number of tokens processed (prompt + completion)",
        {"provider", "model", "consumer", "route"})

    metrics.request_count = prometheus:counter("requests_total",
        "Total number of AI requests",
        {"provider", "model", "consumer", "route", "status"})

    metrics.latency = prometheus:histogram("latency_ms",
        "Request latency in milliseconds",
        {"provider", "model", "consumer", "route"},
        DEFAULT_BUCKETS)

    metrics.cost = prometheus:histogram("cost_usd_total",
        "Total cost in USD",
        {"provider", "model", "consumer", "route"},
        DEFAULT_COST_BUCKETS)

    return prometheus
end

function _M.init()
    if ngx.get_phase() ~= "init" and ngx.get_phase() ~= "init_worker" then
        return
    end
    init_prometheus()
end

function _M.init_worker()
    init_prometheus()
end

local function get_redis_client(conf)
    if redis_client then
        return redis_client
    end

    local redis_conf = conf.redis
    local redis = require("resty.redis")
    local client = redis.new()

    client:set_timeout(3000)

    local ok, err = client:connect(redis_conf.host, redis_conf.port)
    if not ok then
        core.log.error("failed to connect to redis: ", err)
        return nil
    end

    if redis_conf.password and redis_conf.password ~= "" then
        local ok, err = client:auth(redis_conf.password)
        if not ok then
            core.log.error("failed to auth redis: ", err)
            return nil
        end
    end

    if redis_conf.database ~= 0 then
        client:select(redis_conf.database)
    end

    redis_client = client
    return client
end

local function get_token_limit_key(conf, ctx, token_type)
    local redis_conf = conf.redis
    local uid = ctx.var.remote_addr or "anonymous"
    return redis_conf.key_prefix .. token_type .. ":" .. uid
end

local function check_token_limit(conf, ctx, token_type, limit)
    if limit == 0 then
        return true, 0
    end

    local client = get_redis_client(conf)
    if not client then
        core.log.warn("redis client not available, skip rate limiting")
        return true, 0
    end

    local key = get_token_limit_key(conf, ctx, token_type)
    local current, err = client:get(key)
    if err then
        core.log.error("failed to get token limit from redis: ", err)
        return true, 0
    end

    current = tonumber(current) or 0

    if current >= limit then
        return false, limit - current
    end

    return true, limit - current
end

local function increment_token_count(conf, ctx, token_type, count)
    local client = get_redis_client(conf)
    if not client then
        return
    end

    local key = get_token_limit_key(conf, ctx, token_type)
    local ok, err = client:incrby(key, count)
    if not ok then
        core.log.error("failed to increment token count: ", err)
        return
    end

    client:expire(key, 60)
end

local function update_prometheus_metrics(conf, ctx, token_usage)
    if not prometheus or not token_usage then
        return
    end

    local prompt_tokens = token_usage.prompt_tokens or 0
    local completion_tokens = token_usage.completion_tokens or 0
    local total_tokens = token_usage.total_tokens or 0

    local provider = ctx.picked_ai_instance and ctx.picked_ai_instance.provider or "unknown"
    local model = ctx.picked_ai_instance and ctx.picked_ai_instance.options
                    and ctx.picked_ai_instance.options.model or "unknown"

    local consumer_name = "none"
    if ctx.consumer and ctx.consumer.username then
        consumer_name = ctx.consumer.username
    end

    local route_id = "none"
    if ctx.route_id then
        route_id = tostring(ctx.route_id)
    elseif ctx.matched_route and ctx.matched_route.value and ctx.matched_route.value.id then
        route_id = tostring(ctx.matched_route.value.id)
    end

    local latency = 0
    if ctx.var.llm_time_to_first_token then
        latency = tonumber(ctx.var.llm_time_to_first_token) or 0
    end

    local status = "success"
    if ctx.var.llm_request_done == "false" then
        status = "incomplete"
    end

    metrics.prompt_tokens:inc(prompt_tokens,
        {provider, model, consumer_name, route_id})

    metrics.completion_tokens:inc(completion_tokens,
        {provider, model, consumer_name, route_id})

    metrics.total_tokens:inc(total_tokens,
        {provider, model, consumer_name, route_id})

    metrics.request_count:inc(1,
        {provider, model, consumer_name, route_id, status})

    metrics.latency:observe(latency,
        {provider, model, consumer_name, route_id})

    local cost = 0
    if conf.pricing then
        local prompt_cost = (prompt_tokens / 1000000) * (conf.pricing.prompt_tokens or 0)
        local completion_cost = (completion_tokens / 1000000) * (conf.pricing.completion_tokens or 0)
        cost = prompt_cost + completion_cost
    end

    if cost > 0 then
        metrics.cost:observe(cost,
            {provider, model, consumer_name, route_id})
    end
end

function _M.check_schema(conf, schema_type)
    if schema_type == core.schema.TYPE_METADATA then
        return core.schema.check(metadata_schema, conf)
    end
    return core.schema.check(schema, conf)
end

function _M.log(conf, ctx)
    local token_usage = ctx.ai_token_usage
    if not token_usage then
        core.log.warn("ai-token-counter: no token usage found in context")
        return
    end

    local prompt_tokens = token_usage.prompt_tokens or 0
    local completion_tokens = token_usage.completion_tokens or 0
    local total_tokens = token_usage.total_tokens or 0

    local provider = ctx.picked_ai_instance and ctx.picked_ai_instance.provider or "unknown"
    local model = ctx.picked_ai_instance and ctx.picked_ai_instance.options
                    and ctx.picked_ai_instance.options.model or "unknown"

    if conf.limit then
        if conf.limit.total_tokens > 0 then
            local allowed, remaining = check_token_limit(conf, ctx, "total", conf.limit.total_tokens)
            if not allowed then
                core.log.warn("ai-token-counter: total tokens limit exceeded, ",
                    "limit=", conf.limit.total_tokens, ", remaining=", remaining)
            else
                increment_token_count(conf, ctx, "total", total_tokens)
            end
        end

        if conf.limit.prompt_tokens > 0 then
            local allowed, remaining = check_token_limit(conf, ctx, "prompt", conf.limit.prompt_tokens)
            if not allowed then
                core.log.warn("ai-token-counter: prompt tokens limit exceeded, ",
                    "limit=", conf.limit.prompt_tokens, ", remaining=", remaining)
            else
                increment_token_count(conf, ctx, "prompt", prompt_tokens)
            end
        end

        if conf.limit.completion_tokens > 0 then
            local allowed, remaining = check_token_limit(conf, ctx, "completion", conf.limit.completion_tokens)
            if not allowed then
                core.log.warn("ai-token-counter: completion tokens limit exceeded, ",
                    "limit=", conf.limit.completion_tokens, ", remaining=", remaining)
            else
                increment_token_count(conf, ctx, "completion", completion_tokens)
            end
        end
    end

    local cost = 0
    if conf.pricing then
        local prompt_cost = (prompt_tokens / 1000000) * (conf.pricing.prompt_tokens or 0)
        local completion_cost = (completion_tokens / 1000000) * (conf.pricing.completion_tokens or 0)
        cost = prompt_cost + completion_cost
    end

    if conf.log and conf.log.enabled then
        core.log.info("ai-token-counter | provider=", provider,
            " | model=", model,
            " | prompt_tokens=", prompt_tokens,
            " | completion_tokens=", completion_tokens,
            " | total_tokens=", total_tokens,
            " | cost=$", string.format("%.6f", cost))
    end

    if conf.log and conf.log.response_headers then
        ngx.header["X-AI-Prompt-Tokens"] = prompt_tokens
        ngx.header["X-AI-Completion-Tokens"] = completion_tokens
        ngx.header["X-AI-Total-Tokens"] = total_tokens
        if cost > 0 then
            ngx.header["X-AI-Cost"] = string.format("%.6f", cost)
        end
    end

    if conf.log and conf.log.prometheus then
        update_prometheus_metrics(conf, ctx, token_usage)
    end
end

function _M.api()
    return {
        {
            methods = {"GET"},
            uris = {"/v1/ai-token-counter/metrics"},
            handler = function()
                ngx.header["Content-Type"] = "text/plain; charset=utf-8"
                if prometheus then
                    local data = prometheus:metric_data()
                    ngx.print(data)
                else
                    ngx.print("")
                end
            end,
        },
    }
end

function _M.destroy()
    if redis_client then
        redis_client:close()
    end
    prometheus = nil
    metrics = {}
end

return _M
