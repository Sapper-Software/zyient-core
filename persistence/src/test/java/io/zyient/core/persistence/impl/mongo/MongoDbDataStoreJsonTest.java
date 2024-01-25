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

package io.zyient.core.persistence.impl.mongo;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.model.StringKey;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.Cursor;
import io.zyient.core.persistence.DataStoreManager;
import io.zyient.core.persistence.env.DemoDataStoreEnv;
import io.zyient.core.persistence.impl.mongo.model.MongoSearchEntity;
import io.zyient.core.persistence.impl.mongo.model.MongoTestEntity;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MongoDbDataStoreJsonTest {
    private static final String __CONFIG_FILE = "src/test/resources/mongodb/test-mongo-env.xml";
    private static final String __MONGO_DB_NAME = "test-mongodb";

    private static XMLConfiguration xmlConfiguration = null;
    private static DemoDataStoreEnv env = new DemoDataStoreEnv();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.create(xmlConfiguration);
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
                    dataStore.rollback(false);
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
                    dataStore.rollback(false);
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
            DataStoreManager manager = env.getDataStoreManager();
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
                dataStore.rollback(false);
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
                dataStore.rollback(false);
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
            DataStoreManager manager = env.getDataStoreManager();
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
                    dataStore.rollback(false);
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
            DataStoreManager manager = env.getDataStoreManager();
            assertNotNull(manager);
            MongoDbDataStore dataStore = manager.getDataStore(__MONGO_DB_NAME, MongoDbDataStore.class);
            assertNotNull(dataStore);

            try {
                dataStore.beingTransaction();
                Map<StringKey, MongoSearchEntity> counts = new HashMap<>();
                long stime = System.nanoTime();
                try {
                    for (int ii = 0; ii < 10; ii++) {
                        MongoSearchEntity te = new MongoSearchEntity();
                        te = dataStore.create(te, MongoSearchEntity.class, null);
                        assertTrue(te.getUpdatedTime() > te.getCreatedTime());
                        counts.put(te.entityKey(), te);
                    }
                    dataStore.commit();
                } catch (Exception ex) {
                    dataStore.rollback(false);
                    throw ex;
                }
                Thread.sleep(1000);
                long etime = System.nanoTime();
                String query = String.format("timestamp >= %d AND timestamp <= %d", stime, etime);
                AbstractDataStore.Q q = new AbstractDataStore.Q();
                q.where(query);
                Cursor<StringKey, MongoSearchEntity> result = dataStore.search(q,
                        4,
                        StringKey.class,
                        MongoSearchEntity.class,
                        null);
                assertNotNull(result);
                int count = 0;
                while(true) {
                    List<MongoSearchEntity> r = result.nextPage();
                    if (r == null || r.isEmpty()) {
                        break;
                    }
                    count += r.size();
                }
                assertEquals(counts.size(), count);
            } catch (Exception ex) {
                throw ex;
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}