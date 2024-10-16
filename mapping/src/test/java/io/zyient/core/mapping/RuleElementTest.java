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

package io.zyient.core.mapping;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.beans.MapPropertyDef;
import io.zyient.base.common.utils.beans.TypeRef;
import io.zyient.base.common.utils.beans.TypeRefs;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

class RuleElementTest {
    private enum RuleEnum {
        One, Two, Three
    }

    @Getter
    @Setter
    private static class Nested {
        private String key;
        private int value;

        public Nested() {
            key = UUID.randomUUID().toString();
            Random rnd = new Random(System.nanoTime());
            value = rnd.nextInt();
        }
    }

    @Getter
    @Setter
    private static class RuleTest {
        private String name;
        private double value;
        @TypeRef(type = Nested.class)
        private List<Nested> nested;
        @TypeRefs(refs = {
                @TypeRef(value = MapPropertyDef.REF_NAME_KEY, type = String.class),
                @TypeRef(value = MapPropertyDef.REF_NAME_VALUE, type = Nested.class)
        })
        private Map<String, Nested> map;

        public RuleTest() {
            Random rnd = new Random(System.nanoTime());
            int count = rnd.nextInt(1, 10);
            nested = new ArrayList<>(count);
            map = new HashMap<>(count);
            name = UUID.randomUUID().toString();
            value = rnd.nextDouble();
            for (int ii = 0; ii < count; ii++) {
                Nested n = new Nested();
                nested.add(n);
                map.put(n.key, n);
            }
        }
    }

    @Test
    void run() {
        try {
            RuleTest r = new RuleTest();
            StandardEvaluationContext ctx = new StandardEvaluationContext(r);

            ExpressionParser parser = new SpelExpressionParser();
            String rule = "#root.nested.size() > 0 && #root.value > #root.nested[0].value ? #root.value : #root.nested[0].value";
            Expression exp = parser.parseExpression(rule);
            Object value = exp.getValue(ctx);
            System.out.println(value);

            Map<String, Object> root = new HashMap<>();
            Map<String, Object> r1 = new HashMap<>();
            root.put("r1", r1);
            r1.put("name", "name-1");
            r1.put("value", 1000);
            Map<String, Object> r2 = new HashMap<>();
            root.put("r2", r2);
            r2.put("name", "name-2");
            r2.put("value", 2000);
            rule = "#root['r1'].#this['name'] == 'name-2' ? #root['r1'].#this['value'] : 2000";
            exp = parser.parseExpression(rule);
            ctx = new StandardEvaluationContext(root);
            value = exp.getValue(ctx);
            assertInstanceOf(Integer.class, value);
            System.out.println(value);

        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

}