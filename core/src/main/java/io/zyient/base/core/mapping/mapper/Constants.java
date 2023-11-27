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

import io.zyient.base.core.model.PropertyBag;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.*;

public class Constants {
    public enum TestEnum {
        ONE, TWO, THREE, FOUR
    }

    @Getter
    @Setter
    public static class Source {
        private TestEnum type;
        public String id;
        public List<String> values;
        public Source1Nested nested;

        public Source() {
            id = UUID.randomUUID().toString();
            Random rnd = new Random(System.nanoTime());
            int e = rnd.nextInt(0, 3);
            type = TestEnum.values()[e];
            nested = new Source1Nested();
            int count = rnd.nextInt(0, 10);
            if (count > 0) {
                values = new ArrayList<>();
                for (int ii = 0; ii < count; ii++) {
                    values.add(UUID.randomUUID().toString());
                }
            }
        }
    }

    @Getter
    @Setter
    public static class Source1Nested {
        private String id;
        private Date date;
        private Source2Nested nested;

        public Source1Nested() {
            id = UUID.randomUUID().toString();
            nested = new Source2Nested(id);
            Random rnd = new Random(System.currentTimeMillis());
            long v = rnd.nextLong(0, System.currentTimeMillis());
            date = new Date(v);
        }
    }

    @Getter
    @Setter
    public static class Source2Nested {
        private String id;
        private String parentId;
        private List<Double> values;

        public Source2Nested(@NonNull String parentId) {
            this.parentId = parentId;
            id = UUID.randomUUID().toString();
            Random rnd = new Random(System.nanoTime());
            int count = rnd.nextInt(0, 10);
            if (count > 0) {
                values = new ArrayList<>();
                for (int ii = 0; ii < count; ii++) {
                    values.add(rnd.nextDouble());
                }
            }
        }
    }

    @Getter
    @Setter
    public static class Target implements PropertyBag {
        private String sourceId;
        private Date created;
        private TargetNested inner;
        private Map<String, Object> properties;

        @Override
        public Map<String, Object> getProperties() {
            return properties;
        }

        @Override
        public void setProperties(Map<String, Object> properties) {
            this.properties = properties;
        }

        @Override
        public boolean hasProperty(@NonNull String name) {
            if (properties != null) {
                return properties.containsKey(name);
            }
            return false;
        }

        @Override
        public Object getProperty(@NonNull String name) {
            if (properties != null) {
                return properties.get(name);
            }
            return null;
        }

        @Override
        public PropertyBag setProperty(@NonNull String name, @NonNull Object value) {
            if (properties == null) {
                properties = new HashMap<>();
            }
            properties.put(name, value);
            return this;
        }
    }

    @Getter
    @Setter
    public static class TargetNested {
        private String id;
        private TestEnum etype;
        private List<String> strings;
        private List<Double> doubles;
    }
}
