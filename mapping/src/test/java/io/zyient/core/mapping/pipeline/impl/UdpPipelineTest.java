/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.core.mapping.pipeline.impl;

import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPath;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.IOUtils;
import io.zyient.base.common.utils.JSONUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class UdpPipelineTest {
    private static final String __JSON_FILE = "src/test/resources/data/sample_udp.json";

    @Test
    @SuppressWarnings("unchecked")
    void evaluate() {
        try {
            String json = IOUtils.readContent(new File(__JSON_FILE));
            Map<String, Object> jMap = JSONUtils.read(json, Map.class);
            String f = "";
            Filter filter = filter(where("category").is("INCOME STATEMENT").and("pageNo").is(1));
            f = filter.toString();
            Object ret = JsonPath.read(jMap, "$.documents[*].pages[?]", filter);
            assertNotNull(ret);
            ret = JsonPath.read(jMap, "$.documents[*]");
            assertNotNull(ret);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}