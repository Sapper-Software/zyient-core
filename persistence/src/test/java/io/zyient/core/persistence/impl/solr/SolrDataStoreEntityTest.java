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

package io.zyient.core.persistence.impl.solr;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.model.LongKey;
import io.zyient.core.persistence.DataStoreManager;
import io.zyient.core.persistence.env.DemoDataStoreEnv;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SolrDataStoreEntityTest {
    private static final String __CONFIG_FILE = "src/test/resources/solr/test-solr-env.xml";
    private static final String __SOLR_DB_NAME = "test-solr";

    private static XMLConfiguration xmlConfiguration = null;
    private static DemoDataStoreEnv env = new DemoDataStoreEnv();
    private static long startValue = System.nanoTime();

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
            SolrDataStore dataStore = manager.getDataStore(__SOLR_DB_NAME, SolrDataStore.class);
            assertNotNull(dataStore);
            for (int ii = 0; ii < 4; ii++) {
                TestPOJO tp = new TestPOJO(startValue++);
                tp.getState().setState(EEntityState.New);
                tp = dataStore.create(tp, tp.getClass(), null);
                assertTrue(tp.getUpdatedTime() > tp.getCreatedTime());
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
        try {
            DataStoreManager manager = env.getDataStoreManager();
            assertNotNull(manager);
            SolrDataStore dataStore = manager.getDataStore(__SOLR_DB_NAME, SolrDataStore.class);
            assertNotNull(dataStore);
            List<LongKey> keys = new ArrayList<>();
            for (int ii = 0; ii < 4; ii++) {
                TestPOJO tp = new TestPOJO(startValue++);
                tp.getState().setState(EEntityState.New);
                tp = dataStore.create(tp, tp.getClass(), null);
                assertTrue(tp.getUpdatedTime() > tp.getCreatedTime());
                keys.add(tp.entityKey());
            }
            for (LongKey key : keys) {
                boolean r = dataStore.delete(key, TestPOJO.class, null);
                assertTrue(r);
            }
            for (LongKey key : keys) {
                TestPOJO tp = dataStore.find(key, TestPOJO.class, null);
                assertNull(tp);
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
            SolrDataStore dataStore = manager.getDataStore(__SOLR_DB_NAME, SolrDataStore.class);
            assertNotNull(dataStore);
            List<LongKey> keys = new ArrayList<>();
            for (int ii = 0; ii < 4; ii++) {
                TestPOJO tp = new TestPOJO(startValue++);
                tp.getState().setState(EEntityState.New);
                tp = dataStore.create(tp, tp.getClass(), null);
                assertTrue(tp.getUpdatedTime() > tp.getCreatedTime());
                keys.add(tp.entityKey());
            }
            for (LongKey key : keys) {
                TestPOJO tp = dataStore.find(key, TestPOJO.class, null);
                assertNotNull(tp);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void doSearch() {
    }
}