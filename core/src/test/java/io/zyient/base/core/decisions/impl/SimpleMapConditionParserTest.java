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

package io.zyient.base.core.decisions.impl;

import io.zyient.base.core.decisions.Condition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class SimpleMapConditionParserTest {

    @Test
    void parse() {
        try {
            Map<String, Object> data = new HashMap<>();
            data.put("string", "string-01");
            data.put("int", 12398);
            data.put("intString", "122");
            data.put("double", 888.99);
            data.put("doubleString", "1234.567");
            Map<String, Object> inner = new HashMap<>();
            inner.put("nested-string", "string-02");
            inner.put("nested-long", 1288);
            data.put("inner", inner);

            SimpleMapConditionParser parser = new SimpleMapConditionParser();
            Condition<Map<String, Object>> condition = parser.parse("< 'string-02'", "['string']", String.class, null);
            assertTrue(condition.evaluate(data));
            condition = parser.parse(">= 'string-02'", "['inner'].['nested-string']", String.class, null);
            assertTrue(condition.evaluate(data));
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex);
        }
    }
}