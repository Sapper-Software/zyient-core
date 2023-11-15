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

package io.zyient.base.core.stores.impl.rdbms;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.stores.DataStoreEnv;
import io.zyient.base.core.stores.DataStoreManager;
import io.zyient.base.core.stores.impl.rdbms.model.CustomersEntity;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class RdbmsDataStoreTest {
    private static final String __CONFIG_FILE = "src/test/resources/rdbms/test-mariadb-env.xml";
    private static final String __DATASTORE = "test-rdbms";
    private static XMLConfiguration xmlConfiguration = null;
    private static DataStoreEnv env = new DataStoreEnv();
    private static int SEED = 0;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.init(xmlConfiguration);
        env.connectionManager().save();
    }

    @AfterAll
    public static void stop() throws Exception {
        env.close();
    }

    @Test
    void createEntity() {
        try {
            List<CustomersEntity> customers = createCustomers(5, SEED);
            SEED += customers.size();

            DataStoreManager manager = env.dataStoreManager();
            assertNotNull(manager);
            RdbmsDataStore dataStore = manager.getDataStore(__DATASTORE, RdbmsDataStore.class);
            assertNotNull(dataStore);
            dataStore.beingTransaction();
            try {
                for(CustomersEntity ce : customers) {
                    ce = dataStore.create(ce, CustomersEntity.class, null);
                }
                dataStore.commit();
            } catch (Exception ex) {
                dataStore.rollback();
                throw ex;
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void updateEntity() {
    }

    @Test
    void deleteEntity() {
    }

    @Test
    void findEntity() {
    }

    @Test
    void doSearch() {
    }

    private List<CustomersEntity> createCustomers(int size, int seed) {
        List<CustomersEntity> customers = new ArrayList<>(size);
        Date dt = new Date(System.currentTimeMillis());
        String s = String.format("%d%d%d%d%d",
                dt.getYear() - 110,
                dt.getMonth(),
                dt.getDate(),
                dt.getHours(),
                dt.getMinutes());
        int start = Integer.parseInt(s) + seed;
        for (int ii = 0; ii < size; ii++) {
            start += ii;
            CustomersEntity ce = new CustomersEntity(start);
            customers.add(ce);
        }
        return customers;
    }
}