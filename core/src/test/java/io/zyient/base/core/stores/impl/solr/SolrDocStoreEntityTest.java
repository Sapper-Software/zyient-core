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

package io.zyient.base.core.stores.impl.solr;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.content.model.Document;
import io.zyient.base.core.content.model.DocumentId;
import io.zyient.base.core.content.model.DocumentState;
import io.zyient.base.core.model.StringKey;
import io.zyient.base.core.stores.Cursor;
import io.zyient.base.core.stores.DataStoreEnv;
import io.zyient.base.core.stores.DataStoreManager;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SolrDocStoreEntityTest {
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public static class DemoDocState extends DocumentState<EEntityState> {

        protected DemoDocState() {
            super(EEntityState.Error, EEntityState.New);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public static class TestDocument extends Document<EEntityState, StringKey> {

    }

    private static final String __CONFIG_FILE = "src/test/resources/solr/test-solr-env.xml";
    private static final String __SOLR_DB_NAME = "test-solr";
    private static final String __SOLR_COLLECTION_NAME = "test-db";
    private static final String[] DOCUMENTS = {"src/test/resources/data/business-financial-data-june-2023-quarter-csv.csv",
            "src/test/resources/data/customers_202311231439.csv",
            "src/test/resources/data/search/apache_solr_tutorial.pdf",
            "src/test/resources/data/search/sample-docs.zip",
            "src/test/resources/data/search/hibernate_tutorial.pdf",
            "src/test/resources/data/Financial Sample.xlsx",
            "src/test/resources/data/iso_countries.csv",
            "src/test/resources/data/customers_202311231645.xml",
            "src/test/resources/data/employees_202311232000.json"
    };
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

    //@Test
    @SuppressWarnings("unchecked")
    void createEntity() {
        try {
            DataStoreManager manager = env.dataStoreManager();
            assertNotNull(manager);
            SolrDataStore dataStore = manager.getDataStore(__SOLR_DB_NAME, SolrDataStore.class);
            assertNotNull(dataStore);
            for (String source : DOCUMENTS) {
                File path = new File(source);
                Document<EEntityState, StringKey> doc = new Document<>();
                doc.setId(new DocumentId(__SOLR_COLLECTION_NAME));
                doc.setName(source);
                doc.setDocState(new DemoDocState());
                doc.getDocState().setState(EEntityState.New);
                doc.setPath(path);
                doc.setUri(path.toURI().toString());
                doc.setCreatedBy("DEMO");
                doc.setModifiedBy("DEMO");

                doc = dataStore.create(doc, doc.getClass(), null);
                assertNotNull(doc);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void findEntity() {
        try {
            DataStoreManager manager = env.dataStoreManager();
            assertNotNull(manager);
            SolrDataStore dataStore = manager.getDataStore(__SOLR_DB_NAME, SolrDataStore.class);
            assertNotNull(dataStore);
            List<DocumentId> ids = new ArrayList<>();
            for (String source : DOCUMENTS) {
                if (!source.endsWith(".pdf")) continue;
                File path = new File(source);
                Document<EEntityState, StringKey> doc = new Document<>();
                doc.setId(new DocumentId(__SOLR_COLLECTION_NAME));
                doc.setName(source);
                doc.setDocState(new DemoDocState());
                doc.getDocState().setState(EEntityState.New);
                doc.setPath(path);
                doc.setUri(path.toURI().toString());
                doc.setCreatedBy("DEMO");
                doc.setModifiedBy("DEMO");
                doc.setProperty("TEST-LONG", System.nanoTime());
                doc.setProperty("TEST-ENUM", doc.getDocState().getState());
                doc.setProperty("TEST-DATE", new Date(System.currentTimeMillis()));

                doc = dataStore.create(doc, doc.getClass(), null);
                assertNotNull(doc);
                ids.add(doc.entityKey());
            }
            for (DocumentId id : ids) {
                Document<EEntityState, StringKey> doc = dataStore.find(id, Document.class, null);
                assertNotNull(doc);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

   // @Test
    @SuppressWarnings("unchecked")
    void doSearch() {
        try {
            DataStoreManager manager = env.dataStoreManager();
            assertNotNull(manager);
            SolrDataStore dataStore = manager.getDataStore(__SOLR_DB_NAME, SolrDataStore.class);
            assertNotNull(dataStore);
            for (String source : DOCUMENTS) {
                if (!source.endsWith(".pdf")) continue;
                File path = new File(source);
                Document<EEntityState, StringKey> doc = new Document<>();
                doc.setId(new DocumentId(__SOLR_COLLECTION_NAME));
                doc.setName(source);
                doc.setDocState(new DemoDocState());
                doc.getDocState().setState(EEntityState.New);
                doc.setPath(path);
                doc.setUri(path.toURI().toString());
                doc.setCreatedBy("DEMO");
                doc.setModifiedBy("DEMO");

                doc = dataStore.create(doc, doc.getClass(), null);
                assertNotNull(doc);
            }
            DocumentQueryBuilder builder = new DocumentQueryBuilder(Document.class,
                    new StandardAnalyzer(),
                    __SOLR_COLLECTION_NAME);
            builder.matches("Clean and simple SQL processing.");
            Cursor<DocumentId, TestDocument> cursor = dataStore.search(builder.build(),
                    DocumentId.class,
                    TestDocument.class,
                    null);
            int count = 0;
            while (true) {
                List<TestDocument> docs = cursor.nextPage();
                if (docs == null || docs.isEmpty()) break;
                count += docs.size();
            }
            assertTrue(count > 0);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}