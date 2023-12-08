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
import io.zyient.core.persistence.Cursor;
import io.zyient.core.persistence.DataStoreManager;
import io.zyient.core.persistence.env.DemoDataStoreEnv;
import io.zyient.core.persistence.impl.solr.model.DemoTestDocument;
import io.zyient.core.persistence.model.Document;
import io.zyient.core.persistence.model.DocumentId;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SolrDocStoreEntityTest {


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
    @SuppressWarnings("unchecked")
    void createEntity() {
        try {
            DataStoreManager manager = env.getDataStoreManager();
            assertNotNull(manager);
            SolrDataStore dataStore = manager.getDataStore(__SOLR_DB_NAME, SolrDataStore.class);
            assertNotNull(dataStore);
            for (String source : DOCUMENTS) {
                File path = new File(source);
                DemoTestDocument doc = new DemoTestDocument();
                doc.setId(new DocumentId(__SOLR_COLLECTION_NAME));
                doc.setName(source);
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
    void findEntity() {
        try {
            DataStoreManager manager = env.getDataStoreManager();
            assertNotNull(manager);
            SolrDataStore dataStore = manager.getDataStore(__SOLR_DB_NAME, SolrDataStore.class);
            assertNotNull(dataStore);
            List<DocumentId> ids = new ArrayList<>();
            for (String source : DOCUMENTS) {
                if (!source.endsWith(".pdf")) continue;
                File path = new File(source);
                DemoTestDocument doc = new DemoTestDocument();
                doc.setId(new DocumentId(__SOLR_COLLECTION_NAME));
                doc.setName(source);
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
                DemoTestDocument doc = dataStore.find(id, DemoTestDocument.class, null);
                assertNotNull(doc);
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
            SolrDataStore dataStore = manager.getDataStore(__SOLR_DB_NAME, SolrDataStore.class);
            assertNotNull(dataStore);
            Map<String, Integer> docMap = new HashMap<>();
            for (String source : DOCUMENTS) {
                if (!source.endsWith(".pdf")) continue;
                File path = new File(source);
                DemoTestDocument doc = new DemoTestDocument();
                doc.setId(new DocumentId(__SOLR_COLLECTION_NAME));
                doc.setName(source);
                doc.getDocState().setState(EEntityState.New);
                doc.setPath(path);
                doc.setUri(path.toURI().toString());
                doc.setCreatedBy("DEMO");
                doc.setModifiedBy("DEMO");
                for (String d : DOCUMENTS) {
                    if (d.endsWith(".pdf")) continue;
                    File cp = new File(source);
                    DemoTestDocument cd = new DemoTestDocument();
                    cd.setId(new DocumentId(__SOLR_COLLECTION_NAME));
                    cd.setName(d);
                    cd.getDocState().setState(EEntityState.New);
                    cd.setPath(cp);
                    cd.setUri(cp.toURI().toString());
                    cd.setCreatedBy("DEMO");
                    cd.setModifiedBy("DEMO");
                    doc.add(cd);
                }
                doc = dataStore.create(doc, doc.getClass(), null);
                docMap.put(doc.getSearchId(), doc.getDocuments().size());
                assertNotNull(doc);
            }
            DocumentQueryBuilder builder = new DocumentQueryBuilder(Document.class,
                    new StandardAnalyzer(),
                    __SOLR_COLLECTION_NAME);
            builder.matches("Clean and simple SQL processing.");
            Cursor<DocumentId, DemoTestDocument> cursor = dataStore.search(builder.build(),
                    DocumentId.class,
                    DemoTestDocument.class,
                    null);
            int count = 0;
            while (true) {
                List<DemoTestDocument> docs = cursor.nextPage();
                if (docs == null || docs.isEmpty()) break;
                for (DemoTestDocument doc : docs) {
                    if (docMap.containsKey(doc.getSearchId())) {
                        int size = docMap.get(doc.getSearchId());
                        if (doc.getDocuments() == null && size > 0) {
                            throw new Exception(String.format("Nested documents is NULL. [expected=%d]", size));
                        }
                        if (size > 0) {
                            assertEquals(size, doc.getDocuments().size());
                        } else {
                            assertNull(doc.getDocuments());
                        }
                    }
                }
                count += docs.size();
            }
            assertTrue(count > 0);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}