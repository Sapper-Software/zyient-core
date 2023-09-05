/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.base.core.connections.settings;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.settings.kafka.KafkaPartitionsParser;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class KafkaPartitionsParserTest {

    @Test
    void parse() {
        try {
            KafkaPartitionsParser parser = new KafkaPartitionsParser();
            String s = "10";
            List<Integer> parts = parser.parse(s);
            assertEquals(10, parts.get(0));
            s = "10;20";
            parts = parser.parse(s);
            assertEquals(10, parts.get(0));
            assertEquals(20, parts.get(1));
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}