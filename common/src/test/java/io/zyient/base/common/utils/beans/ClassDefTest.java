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

package io.zyient.base.common.utils.beans;

import com.google.common.base.Strings;
import io.zyient.base.common.model.EReaderType;
import io.zyient.base.common.utils.ReflectionHelper;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ClassDefTest {
    public interface Dated {
        Date getDate();

        void setDate(Date date);

        String getName();

        void setName(String name);
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class Nested implements Dated {
        private String name;
        private Date date;
        @TypeRefs(refs = {
                @TypeRef(value = MapPropertyDef.REF_NAME_KEY, type = String.class),
                @TypeRef(value = MapPropertyDef.REF_NAME_VALUE, type = Nested.class)
        })
        private Map<String, Dated> dates;
        private EReaderType type;

        @Override
        public Date getDate() {
            return date;
        }

        @Override
        public void setDate(Date date) {
            this.date = date;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void setName(String name) {
            this.name = name;
        }

        public Nested() {
        }

        public Nested(int count) {
            name = "id::" + count;
            date = new Date(System.currentTimeMillis());
            dates = new HashMap<>();
            for (int ii = 0; ii < count; ii++) {
                Nested n = new Nested(0);
                dates.put(n.name, n);
            }
        }
    }

    @Getter
    @Setter
    public static class Outer {
        private long id;
        private Nested dated;
        @Setter(AccessLevel.NONE)
        private long version;
        @TypeRef(type = Nested.class)
        private List<Nested> nested;

        public Outer() {
        }

        public Outer(int count) {
            id = System.currentTimeMillis();
            version = count;
            nested = new ArrayList<>(count);
            for (int ii = 0; ii < count; ii++) {
                Nested n = new Nested(ii);
                nested.add(n);
            }
        }
    }

    @Test
    void findSetter() {
        try {
            ClassDef def = BeanUtils.get(Outer.class);
            Field[] fields = ReflectionHelper.getAllFields(Outer.class);
            assertNotNull(fields);
            for (Field field : fields) {
                Method setter = def.findSetter(field.getName(), field.getType());
                if (field.getName().compareTo("version") == 0) {
                    assertNull(setter);
                } else
                    assertNotNull(setter);
            }
            Outer outer = new Outer();
            BeanUtils.setValue(outer, "id", System.nanoTime());
            BeanUtils.setValue(outer, "dated.name", "TEST-OUTER");
            BeanUtils.setValue(outer, "dated.date", new Date(System.currentTimeMillis()));
            assertNotNull(outer.dated);
            assertNotNull(outer.dated.date);
            assertFalse(Strings.isNullOrEmpty(outer.dated.name));
            for (int ii = 0; ii < 3; ii++) {
                BeanUtils.setValue(outer, "nested[" + ii + "].name", "NESTED-" + ii);
                BeanUtils.setValue(outer, "nested[" + ii + "].date", new Date(System.currentTimeMillis()));
                for (int jj = 0; jj < 5; jj++) {
                    BeanUtils.setValue(outer, "nested[" + ii + "].dates(KEY-" + jj + ").name", "NESTED-KEY-" + jj);
                    BeanUtils.setValue(outer, "nested[" + ii + "].dates(KEY-" + jj + ").date", new Date(System.currentTimeMillis()));
                    BeanUtils.setValueFromString(outer, "nested[" + ii + "].dates(KEY-" + jj + ").type", EReaderType.HTTPS.name());
                }
            }
            assertEquals(3, outer.nested.size());
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex);
        }
    }

    @Test
    void findGetter() {
        try {
            ClassDef def = BeanUtils.get(Outer.class);
            Field[] fields = ReflectionHelper.getAllFields(Outer.class);
            assertNotNull(fields);
            for (Field field : fields) {
                Method getter = def.findGetter(field.getName(), field.getType());
                assertNotNull(getter);
            }
            Outer outer = new Outer(3);
            String search = "nested[2].dates(id::0)";
            Object value = BeanUtils.getValue(outer, search);
            assertNotNull(value);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex);
        }
    }
}