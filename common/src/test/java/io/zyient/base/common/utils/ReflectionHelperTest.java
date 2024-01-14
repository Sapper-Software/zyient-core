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

package io.zyient.base.common.utils;

import io.zyient.base.common.utils.beans.BeanUtils;
import io.zyient.base.common.utils.beans.MapPropertyDef;
import io.zyient.base.common.utils.beans.TypeRef;
import io.zyient.base.common.utils.beans.TypeRefs;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReflectionHelperTest {
    @Getter
    @Setter
    public static class NestedObject {
        private String index;
        @TypeRef(type = String.class)
        private List<String> values = new ArrayList<>();

        public NestedObject() {

        }

        public NestedObject(int index) {
            this.index = String.format("%s-%d", getClass().getSimpleName(), index);
            if (index > 0) {
                values = new ArrayList<>(index);
            }
            for (int ii = 0; ii < index; ii++) {
                values.add(String.format("VALUE-%d [%s]", ii, this.index));
            }
        }
    }

    public enum TestEnum {
        A, B, C, D
    }

    @Getter
    @Setter
    public static class TestObject {
        private long id;
        @TypeRefs(refs = {
                @TypeRef(value = MapPropertyDef.REF_NAME_KEY, type = String.class),
                @TypeRef(value = MapPropertyDef.REF_NAME_VALUE, type = NestedObject.class)
        })
        private Map<String, NestedObject> objects = new HashMap<>();
        private TestEnum ev;

        public TestObject() {

        }

        public TestObject(long count) {
            this.id = count;
            int ii = (int) (count % 4);
            ev = TestEnum.values()[ii];
            if (count > 0) {
                objects = new HashMap<>();
                for (int jj = 0; jj < count; jj++) {
                    NestedObject no = new NestedObject(jj);
                    objects.put(no.index, no);
                }
            }
        }
    }

    @Test
    void getFieldValue() {
        try {
            int count = 10;
            List<TestObject> objects = new ArrayList<>(count);
            for (int ii = 0; ii < count; ii++) {
                objects.add(new TestObject(ii));
            }
            for (int ii = 0; ii < count; ii++) {
                TestObject te = objects.get(ii);
                Object ret = BeanUtils.getValue(te, "ev");
                assertNotNull(te);
                if (ii > 0) {
                    for (String key : te.objects.keySet()) {
                        Object o = BeanUtils.getValue(te, String.format("objects(%s).index", key));
                        assertInstanceOf(String.class, o);
                        assertEquals(key, o);
                        if (te.objects.get(key).values != null) {
                            int jj = te.objects.get(key).values.size() - 1;
                            if (jj >= 0) {
                                o = BeanUtils.getValue(te, String.format("objects(%s).values[%d]", key, jj));
                                assertInstanceOf(String.class, o);
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex);
        }
    }

    @Test
    void setFieldValue() {
        try {
            TestObject te = new TestObject();
            for (int ii = 0; ii < 5; ii++) {
                String key = String.format("KEY-%d", ii);
                BeanUtils.setValue(te, String.format("objects(%s)", key), new NestedObject(10));
                Object val = BeanUtils.getValue(te, String.format("objects(%s).values[%d]", key, 7));
                assertNotNull(val);
                BeanUtils.setValue(te, String.format("objects(%s).index", key), key);
                for (int jj = 0; jj < 3; jj++) {
                    val = BeanUtils.getValue(te, String.format("objects(%s).values[%d]", key, jj));
                    assertNotNull(val);
                }
            }
            System.out.println(JSONUtils.asString(te));
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex);
        }
    }
}