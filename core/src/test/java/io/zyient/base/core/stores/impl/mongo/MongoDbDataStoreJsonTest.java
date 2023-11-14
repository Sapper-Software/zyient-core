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

package io.zyient.base.core.stores.impl.mongo;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.model.StringKey;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.BaseSearchResult;
import io.zyient.base.core.stores.DataStoreEnv;
import io.zyient.base.core.stores.DataStoreManager;
import io.zyient.base.core.stores.impl.mongo.model.MongoSearchEntity;
import io.zyient.base.core.stores.impl.mongo.model.MongoTestEntity;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MongoDbDataStoreJsonTest {
    private static final String __CONFIG_FILE = "src/test/resources/mongodb/test-mongo-env.xml";
    private static final String __MONGO_DB_NAME = "test-mongodb";

    private static XMLConfiguration xmlConfiguration = null;
    private static DataStoreEnv env = new DataStoreEnv();

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
            DataStoreManager manager = env.dataStoreManager();
            assertNotNull(manager);
            MongoDbDataStore dataStore = manager.getDataStore(__MONGO_DB_NAME, MongoDbDataStore.class);
            assertNotNull(dataStore);

            dataStore.beingTransaction();
            try {
                for (int ii = 0; ii < 10; ii++) {
                    MongoTestEntity te = new MongoTestEntity();
                    te = dataStore.createEntity(te, MongoTestEntity.class, null);
                    assertTrue(te.getUpdatedTime() > te.getCreatedTime());
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
        try {
            DataStoreManager manager = env.dataStoreManager();
            assertNotNull(manager);
            MongoDbDataStore dataStore = manager.getDataStore(__MONGO_DB_NAME, MongoDbDataStore.class);
            assertNotNull(dataStore);

            try {
                dataStore.beingTransaction();
                Map<StringKey, Integer> counts = new HashMap<>();
                try {
                    for (int ii = 0; ii < 10; ii++) {
                        MongoTestEntity te = new MongoTestEntity();
                        te = dataStore.createEntity(te, MongoTestEntity.class, null);
                        assertTrue(te.getUpdatedTime() > te.getCreatedTime());
                        if (te.getNested() != null)
                            counts.put(te.entityKey(), te.getNested().size());
                        else
                            counts.put(te.entityKey(), 0);
                    }
                    dataStore.commit();
                } catch (Exception ex) {
                    dataStore.rollback();
                    throw ex;
                }
                dataStore.beingTransaction();
                try {
                    for (StringKey key : counts.keySet()) {
                        MongoTestEntity te = dataStore.find(key, MongoTestEntity.class, null);
                        assertNotNull(te);
                        int c = counts.get(key);
                        if (c > 0)
                            assertEquals(te.getNested().size(), c);
                        if (te.getNested() != null)
                            te.getNested().clear();
                        te = dataStore.update(te, MongoTestEntity.class, null);
                    }
                    dataStore.commit();
                } catch (Exception ex) {
                    dataStore.rollback();
                    throw ex;
                }
                for (StringKey key : counts.keySet()) {
                    MongoTestEntity te = dataStore.findEntity(key, MongoTestEntity.class, null);
                    assertNotNull(te);
                    assertTrue(te.getNested() == null || te.getNested().isEmpty());
                }
            } catch (Exception ex) {
                throw ex;
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void deleteEntity() {
        try {
            DataStoreManager manager = env.dataStoreManager();
            assertNotNull(manager);
            MongoDbDataStore dataStore = manager.getDataStore(__MONGO_DB_NAME, MongoDbDataStore.class);
            assertNotNull(dataStore);

            dataStore.beingTransaction();
            Map<StringKey, Integer> counts = new HashMap<>();
            try {
                for (int ii = 0; ii < 10; ii++) {
                    MongoTestEntity te = new MongoTestEntity();
                    te = dataStore.create(te, MongoTestEntity.class, null);
                    assertTrue(te.getUpdatedTime() > te.getCreatedTime());
                    if (te.getNested() != null)
                        counts.put(te.entityKey(), te.getNested().size());
                    else
                        counts.put(te.entityKey(), 0);
                }
                dataStore.commit();
            } catch (Exception ex) {
                dataStore.rollback();
                throw ex;
            }
            dataStore.beingTransaction();
            try {
                for (StringKey key : counts.keySet()) {
                    boolean r = dataStore.delete(key, MongoTestEntity.class, null);
                    assertTrue(r);
                }
                dataStore.commit();
            } catch (Exception ex) {
                dataStore.rollback();
                throw ex;
            }
            for (StringKey key : counts.keySet()) {
                MongoTestEntity te = dataStore.find(key, MongoTestEntity.class, null);
                assertNull(te);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void findEntity() {
        try {
            DataStoreManager manager = env.dataStoreManager();
            assertNotNull(manager);
            MongoDbDataStore dataStore = manager.getDataStore(__MONGO_DB_NAME, MongoDbDataStore.class);
            assertNotNull(dataStore);

            try {
                dataStore.beingTransaction();
                Map<StringKey, Integer> counts = new HashMap<>();
                try {
                    for (int ii = 0; ii < 10; ii++) {
                        MongoTestEntity te = new MongoTestEntity();
                        te = dataStore.create(te, MongoTestEntity.class, null);
                        assertTrue(te.getUpdatedTime() > te.getCreatedTime());
                        if (te.getNested() != null)
                            counts.put(te.entityKey(), te.getNested().size());
                        else
                            counts.put(te.entityKey(), 0);
                    }
                    dataStore.commit();
                } catch (Exception ex) {
                    dataStore.rollback();
                    throw ex;
                }
                for (StringKey key : counts.keySet()) {
                    MongoTestEntity te = dataStore.find(key, MongoTestEntity.class, null);
                    assertNotNull(te);
                    int c = counts.get(key);
                    if (c > 0)
                        assertEquals(te.getNested().size(), c);
                }
            } catch (Exception ex) {
                throw ex;
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void doSearch() {
        try {
            DataStoreManager manager = env.dataStoreManager();
            assertNotNull(manager);
            MongoDbDataStore dataStore = manager.getDataStore(__MONGO_DB_NAME, MongoDbDataStore.class);
            assertNotNull(dataStore);

            try {
                dataStore.beingTransaction();
                Map<StringKey, MongoSearchEntity> counts = new HashMap<>();
                try {
                    for (int ii = 0; ii < 10; ii++) {
                        MongoSearchEntity te = new MongoSearchEntity();
                        te = dataStore.create(te, MongoSearchEntity.class, null);
                        assertTrue(te.getUpdatedTime() > te.getCreatedTime());
                        counts.put(te.entityKey(), te);
                    }
                    dataStore.commit();
                } catch (Exception ex) {
                    dataStore.rollback();
                    throw ex;
                }
                for (StringKey key : counts.keySet()) {
                    MongoSearchEntity te = counts.get(key);
                    String query = String.format("textValue='%s' AND timestamp=%d AND nested.parentId = '%s'",
                            te.getTextValue(), te.getTimestamp(), te.getNested().getParentId());
                    AbstractDataStore.Q q = new AbstractDataStore.Q();
                    q.where(query);
                    BaseSearchResult<MongoSearchEntity> result = dataStore.search(q,
                            StringKey.class,
                            MongoSearchEntity.class,
                            null);
                    assertNotNull(result);
                    assertEquals(result.getCount(), 1);
                }
            } catch (Exception ex) {
                throw ex;
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}