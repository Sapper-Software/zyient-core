/*
 * Copyright(C) (2024) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.base.common.json;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.zyient.base.common.utils.JSONUtils;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ClassNameDeSerializerTest {
    @Getter
    @Setter
    public static class TestSerDe {
        @JsonDeserialize(using = ClassNameDeSerializer.class)
        @JsonSerialize(keyUsing = ClassNameSerializer.class)
        private Class<?> type;
    }

    @Test
    void deserialize() {
        try {
            TestSerDe t = new TestSerDe();
            t.setType(ClassCastException.class);

            String json = JSONUtils.asString(t);
            t = JSONUtils.read(json, TestSerDe.class);
            assertEquals(ClassCastException.class, t.type);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex);
        }
    }
}