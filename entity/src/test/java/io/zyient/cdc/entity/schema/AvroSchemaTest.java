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

package io.zyient.cdc.entity.schema;

import io.zyient.base.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AvroSchemaTest {
    private static final String SCHEMA_1 = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"record\",\n" +
            "  \"namespace\" : \"ai.sapper.hcdc\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"genres\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  }, {\n" +
            "    \"name\" : \"movieId\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  }, {\n" +
            "    \"name\" : \"title\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  } ]\n" +
            "}";

    private static final String SCHEMA_2 = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"record\",\n" +
            "  \"namespace\" : \"ai.sapper.hcdc\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"genres\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  }, {\n" +
            "    \"name\" : \"movieId\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  }, {\n" +
            "    \"name\" : \"ADDED_COLUMN_UUID\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  }, {\n" +
            "    \"name\" : \"title\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  } ]\n" +
            "}";

    @Test
    void compare() {
        try {
            AvroSchema schema1 = new AvroSchema().withSchemaStr(SCHEMA_1);
            AvroSchema schema2 = new AvroSchema().withSchemaStr(SCHEMA_2);
            AvroSchema schema3 = new AvroSchema().withSchemaStr(SCHEMA_1);

            assertFalse(schema1.compare(schema2));
            assertTrue(schema1.compare(schema3));

        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}