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

package io.zyient.base.core.mapping.transformers;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.mapping.mapper.MappingSettings;
import io.zyient.base.core.mapping.model.EnumMappedElement;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class EnumTransformerTest {
    public enum TestTransform {
        A, B, C, D
    }

    @Test
    void transform() {
        try {
            EnumMappedElement me = new EnumMappedElement();
            me.setEnumType(TestTransform.class);
            me.setEnumMappings(Map.of("ea", "A", "ec", "C"));
            MappingSettings settings = new MappingSettings();
            EnumTransformer transformer = (EnumTransformer) new EnumTransformer()
                    .type(me.getEnumType())
                    .enumValues(me.getEnumMappings())
                    .configure(settings);
            TestTransform v = (TestTransform) transformer.transform("ea");
            assertEquals(TestTransform.A, v);
            v = (TestTransform) transformer.transform("B");
            assertEquals(TestTransform.B, v);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}