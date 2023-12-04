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
import io.zyient.base.core.model.IntegerKey;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.Cursor;
import io.zyient.base.core.stores.DataStoreManager;
import io.zyient.base.core.stores.DemoDataStoreEnv;
import io.zyient.base.core.stores.impl.rdbms.model.CustomersEntity;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RdbmsDataStoreTest {
    private static final String __CONFIG_FILE = "src/test/resources/rdbms/test-mariadb-env.xml";
    private static final String __DATASTORE = "test-rdbms";
    private static XMLConfiguration xmlConfiguration = null;
    private static DemoDataStoreEnv env = new DemoDataStoreEnv();

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
            DataStoreManager manager = env.getDataStoreManager();
            assertNotNull(manager);
            RdbmsDataStore dataStore = manager.getDataStore(__DATASTORE, RdbmsDataStore.class);
            assertNotNull(dataStore);
            dataStore.beingTransaction();
            try {
                List<CustomersEntity> customers = createCustomers(5, dataStore);
                for (CustomersEntity ce : customers) {
                    ce = dataStore.create(ce, CustomersEntity.class, null);
                }
                dataStore.commit();
            } catch (Exception ex) {
                dataStore.rollback(false);
                throw ex;
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void updateEntity() {
        try {
            DataStoreManager manager = env.getDataStoreManager();
            assertNotNull(manager);
            RdbmsDataStore dataStore = manager.getDataStore(__DATASTORE, RdbmsDataStore.class);
            assertNotNull(dataStore);
            List<CustomersEntity> customers = null;
            dataStore.beingTransaction();
            try {
                customers = createCustomers(5, dataStore);
                for (CustomersEntity ce : customers) {
                    ce = dataStore.create(ce, CustomersEntity.class, null);
                }
                dataStore.commit();
            } catch (Exception ex) {
                dataStore.rollback(false);
                throw ex;
            }
            dataStore.beingTransaction();
            try {
                for (CustomersEntity ce : customers) {
                    CustomersEntity fe = dataStore.find(ce.entityKey(), CustomersEntity.class, null);
                    assertNotNull(fe);
                    fe.setCreditLimit(BigDecimal.valueOf(0));
                    fe = dataStore.update(fe, CustomersEntity.class, null);
                    assertNotNull(fe);
                }
                dataStore.commit();
            } catch (Exception ex) {
                dataStore.rollback(false);
                throw ex;
            }
            for (CustomersEntity ce : customers) {
                CustomersEntity fe = dataStore.find(ce.entityKey(), CustomersEntity.class, null);
                assertNotNull(fe);
                assertEquals(0.0, fe.getCreditLimit().doubleValue());
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void deleteEntity() {
        try {
            DataStoreManager manager = env.getDataStoreManager();
            assertNotNull(manager);
            RdbmsDataStore dataStore = manager.getDataStore(__DATASTORE, RdbmsDataStore.class);
            assertNotNull(dataStore);
            List<CustomersEntity> customers = null;
            dataStore.beingTransaction();
            try {
                customers = createCustomers(5, dataStore);
                for (CustomersEntity ce : customers) {
                    ce = dataStore.create(ce, CustomersEntity.class, null);
                }
                dataStore.commit();
            } catch (Exception ex) {
                dataStore.rollback(false);
                throw ex;
            }
            dataStore.beingTransaction();
            try {
                for (CustomersEntity ce : customers) {
                    boolean r = dataStore.delete(ce.entityKey(), CustomersEntity.class, null);
                    assertTrue(r);
                }
                dataStore.commit();
            } catch (Exception ex) {
                dataStore.rollback(false);
                throw ex;
            }
            for (CustomersEntity ce : customers) {
                CustomersEntity fe = dataStore.find(ce.entityKey(), CustomersEntity.class, null);
                assertNull(fe);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void doSearch() {
        try {
            DataStoreManager manager = env.getDataStoreManager();
            assertNotNull(manager);
            RdbmsDataStore dataStore = manager.getDataStore(__DATASTORE, RdbmsDataStore.class);
            assertNotNull(dataStore);
            List<CustomersEntity> customers = null;
            dataStore.beingTransaction();
            try {
                customers = createCustomers(5, dataStore);
                for (CustomersEntity ce : customers) {
                    ce = dataStore.create(ce, CustomersEntity.class, null);
                }
                dataStore.commit();
            } catch (Exception ex) {
                dataStore.rollback(false);
                throw ex;
            }
            String condition = "city = :city AND state = :state";
            Map<String, Object> params = Map.of("city", "Bangalore", "state", "KA");
            AbstractDataStore.Q query = new AbstractDataStore.Q()
                    .where(condition)
                    .addAll(params);
            Cursor<IntegerKey, CustomersEntity> cursor = dataStore.search(query,
                    3,
                    IntegerKey.class,
                    CustomersEntity.class,
                    null);
            int pages = 0;
            int count = 0;
            Map<String, IntegerKey> keys = new HashMap<>();
            while (true) {
                List<CustomersEntity> r = cursor.nextPage();
                if (r == null || r.isEmpty()) {
                    break;
                }
                for (CustomersEntity c : r) {
                    String k = c.getId().stringKey();
                    if (keys.containsKey(k)) {
                        throw new Exception(String.format("Key already exists. [page=%d][count=%d]", pages, count));
                    }
                    keys.put(k, c.entityKey());
                }
                count += r.size();
                pages++;
            }
            assertTrue(pages > 1);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    private List<CustomersEntity> createCustomers(int size, RdbmsDataStore dataStore) throws Exception {
        List<CustomersEntity> customers = new ArrayList<>(size);
        for (int ii = 0; ii < size; ii++) {
            long seqeunce = dataStore.nextSequence(CustomersEntity.class.getSimpleName());
            assertTrue(seqeunce >= 0);
            CustomersEntity ce = new CustomersEntity((int) seqeunce);
            customers.add(ce);
        }
        return customers;
    }
}