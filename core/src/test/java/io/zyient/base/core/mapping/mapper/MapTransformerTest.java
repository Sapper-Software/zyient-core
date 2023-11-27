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

package io.zyient.base.core.mapping.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.mapping.model.MappedElement;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MapTransformerTest {


    @Test
    @SuppressWarnings("unchecked")
    void mapper() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            MapTransformer<Constants.Target> transformer = new MapTransformer<>(Constants.Target.class);
            transformer.add(new MappedElement("id", "sourceId", false, String.class, null, null));
            transformer.add(new MappedElement("nested.date", "created", false, Date.class, null, null));
            transformer.add(new MappedElement("nested.nested.id", "inner.id", false, String.class, null, null));
            transformer.add(new MappedElement("type", "inner.etype", false, Constants.TestEnum.class, null, null));
            transformer.add(new MappedElement("values", "inner.strings", false, List.class, null, null));
            transformer.add(new MappedElement("nested.nested.values", "inner.doubles", false, List.class, null, null));

            Constants.Source source = new Constants.Source();
            String json = JSONUtils.asString(source, Constants.Source.class);
            Object data = mapper.readValue(json, Object.class);
            assertTrue(data instanceof Map<?, ?>);
            Map<String, Object> output = transformer.transform((Map<String, Object>) data);
            assertNotNull(output);
            Constants.Target target = mapper.convertValue(output, Constants.Target.class);
            assertNotNull(target);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}