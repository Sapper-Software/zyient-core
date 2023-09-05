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

package io.zyient.cdc.entity;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.cdc.entity.avro.AvroEntitySchema;
import io.zyient.cdc.entity.types.DataType;
import io.zyient.cdc.entity.types.SizedDataType;
import io.zyient.cdc.entity.types.TextType;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.junit.jupiter.api.Assertions.*;

class DataTypeTest {
    public static final DataType<String> LONGTEXT = new TextType("LONGTEXT", Types.LONGNVARCHAR, Integer.MAX_VALUE * 2L);

    @Test
    void testEquals() {
        try {
            DataType<?> dt = AvroEntitySchema.FLOAT;
            DataType<?> nt = new DataType<>("float", Float.TYPE, Types.FLOAT);
            assertEquals(nt, dt);

            dt = LONGTEXT;
            dt = new SizedDataType<>((SizedDataType<?>) dt, 200);
            assertTrue(dt instanceof TextType);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}