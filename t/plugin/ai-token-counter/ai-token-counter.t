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
local function test_ai_token_counter()
    local core = require("apisix.core")
    local token_counter = require("apisix.plugins.ai-token-counter")

    -- Test schema validation
    local conf = {
        limit = {
            prompt_tokens = 1000,
            completion_tokens = 500,
            total_tokens = 1500,
        },
        pricing = {
            prompt_tokens = 0.5,    -- $0.5 per 1M prompt tokens
            completion_tokens = 1.5, -- $1.5 per 1M completion tokens
        },
        redis = {
            host = "127.0.0.1",
            port = 6379,
            database = 0,
            key_prefix = "ai_token:",
        },
        log = {
            enabled = true,
            response_headers = true,
        },
    }

    local ok, err = token_counter.check_schema(conf)
    if not ok then
        core.log.error("schema check failed: ", err)
        return
    end

    core.log.info("ai-token-counter schema check passed")
end

return test_ai_token_counter
