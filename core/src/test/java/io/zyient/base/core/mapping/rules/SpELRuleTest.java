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

package io.zyient.base.core.mapping.rules;

import io.zyient.base.core.mapping.model.MappedResponse;
import io.zyient.base.core.mapping.rules.spel.SpELRule;
import io.zyient.base.core.stores.impl.rdbms.model.CustomersEntity;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

class SpELRuleTest {

    @Test
    void doEvaluate() {
    }

    @Test
    void setup() {
        try {
            SpELRule<CustomersEntity> rule = new SpELRule<>();
            rule.name("test-setup")
                    .rule("${city} == 'Bangalore' and ${state} == 'KA'? 'IN' : 'UN' ")
                    .setup(new RuleConfig());
            List<CustomersEntity> entities = createCustomers(5);
            for (CustomersEntity entity : entities) {
                MappedResponse<CustomersEntity> response = new MappedResponse<CustomersEntity>(new HashMap<>())
                        .entity(entity);
                rule.doEvaluate(response);
            }
        } catch (Exception ex) {
            fail(ex);
        }
    }

    private List<CustomersEntity> createCustomers(int size) throws Exception {
        List<CustomersEntity> customers = new ArrayList<>(size);
        for (int ii = 0; ii < size; ii++) {
            CustomersEntity ce = new CustomersEntity(ii);
            customers.add(ce);
        }
        return customers;
    }

}