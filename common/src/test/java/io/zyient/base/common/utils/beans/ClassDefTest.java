package io.zyient.base.common.utils.beans;

import io.zyient.base.common.utils.ReflectionHelper;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ClassDefTest {
    public static interface Dated {
        Date getDate();

        String getName();
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class Nested implements Dated {
        private String name;
        private Date date;
        @TypeRefs(refs = {
                @TypeRef(value = MapPropertyDef.REF_NAME_KEY, type = String.class),
                @TypeRef(value = MapPropertyDef.REF_NAME_VALUE, type = Dated.class)
        })
        private Map<String, Dated> dates;

        @Override
        public Date getDate() {
            return date;
        }

        @Override
        public String getName() {
            return name;
        }


    }

    @Getter
    @Setter
    public static class Outer {
        private long id;
        private Dated dated;
        @Setter(AccessLevel.NONE)
        private long version;
        @TypeRef(type = Nested.class)
        private List<Nested> nested;

        public void setDated(Nested dated) {
            this.dated = dated;
        }
    }

    @Test
    void findSetter() {
        try {
            ClassDef def = new ClassDef().from(Outer.class);
            Field[] fields = ReflectionHelper.getAllFields(Outer.class);
            assertNotNull(fields);
            for (Field field : fields) {
                Method setter = def.findSetter(field);
                if (field.getName().compareTo("version") == 0) {
                    assertNull(setter);
                } else
                    assertNotNull(setter);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex);
        }
    }

    @Test
    void findGetter() {
        try {
            ClassDef def = new ClassDef().from(Outer.class);
            Field[] fields = ReflectionHelper.getAllFields(Outer.class);
            assertNotNull(fields);
            for (Field field : fields) {
                Method getter = def.findGetter(field);
                assertNotNull(getter);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex);
        }
    }
}