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

package io.zyient.core.mapping.rules;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.core.mapping.env.DemoDataStoreEnv;
import io.zyient.core.mapping.model.*;
import io.zyient.core.mapping.rules.spel.SpELRule;
import io.zyient.core.mapping.rules.spel.SpELRuleConfig;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

class SpELRuleTest {
    private static final String __CONFIG_FILE = "src/test/resources/mapping/test-mapping-env.xml";

    private static XMLConfiguration xmlConfiguration = null;
    private static DemoDataStoreEnv env = new DemoDataStoreEnv();

    @BeforeAll
    static void beforeAll() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.create(xmlConfiguration);
        env.connectionManager().save();
    }

    @AfterAll
    static void afterAll() throws Exception {
        env.close();
    }

    @Test
    void doEvaluate() {
    }

    @Test
    void setup() {
        try {
            SpELRuleConfig config = new SpELRuleConfig();
            config.setTarget("country");

            SpELRule<MappedResponse<CustomersEntity>> rule = new SpELRule<>();
            rule.name("test-setup")
                    .expression("${city} == 'Bangalore' and ${state} == 'KA'? 'IN' : 'UN' ")
                    .errorCode(1000001)
                    .withEntityType(CustomerMappedResponse.class);
            rule.setup(config);
            List<CustomersEntity> entities = createCustomers(5);
            for (CustomersEntity entity : entities) {
                MappedResponse<CustomersEntity> response = new MappedResponse<CustomersEntity>(new HashMap<>());
                response.setEntity(entity);
                EvaluationStatus status = rule.evaluate(response);
                assertSame(status.status(), StatusCode.Success);
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