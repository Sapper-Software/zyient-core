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

package io.zyient.base.core.content.impl.db;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.content.ContentProvider;
import io.zyient.base.core.model.DemoPrincipal;
import io.zyient.base.core.model.DemoUserContext;
import io.zyient.base.core.stores.DemoDataStoreEnv;
import io.zyient.base.core.stores.impl.solr.model.DemoDocState;
import io.zyient.base.core.stores.impl.solr.model.DemoTestDocument;
import io.zyient.base.core.stores.model.DocumentId;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


class DbContentProviderTest {
    private static final String __COLLECTION_NAME = "azure-demo-1";
    private static final String __CONFIG_FILE = "src/test/resources/content/fs-azure-dl-db-content.xml";
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
    private static DbContentProvider contentProvider;
    private static final DemoUserContext userContext = new DemoUserContext();

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void beforeAll() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.init(xmlConfiguration);
        env.connectionManager().save();

        HierarchicalConfiguration<ImmutableNode> xmlConfig = env.demoConfig().configurationAt(ContentProvider.__CONFIG_PATH);
        Class<? extends ContentProvider> clazz = (Class<? extends ContentProvider>) ConfigReader.readType(xmlConfig);
        if (!clazz.equals(DbContentProvider.class)) {
            throw new Exception(String.format("Invalid DB content provider. [type=%s]", clazz.getCanonicalName()));
        }
        contentProvider = (DbContentProvider) new DbContentProvider()
                .configure(env.demoConfig(), env);
        userContext.user(new DemoPrincipal());
    }

    @AfterAll
    static void afterAll() throws Exception {
        env.close();
    }

    //@Test
    void insertDocs() {
        try {
            for (String source : DOCUMENTS) {
                File path = new File(source);
                DemoTestDocument doc = new DemoTestDocument();
                doc.setId(new DocumentId(__COLLECTION_NAME));
                doc.setName(source);
                doc.setDocState(new DemoDocState());
                doc.getDocState().setState(EEntityState.New);
                doc.setPath(path);
                doc.setUri(path.toURI().toString());
                doc.setCreatedBy("DEMO");
                doc.setModifiedBy("DEMO");
                doc.getState().setState(EEntityState.New);

                doc = (DemoTestDocument) contentProvider.create(doc, userContext);
                assertNotNull(doc);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void findDocs() {
        try {
            List<DocumentId> ids = new ArrayList<>();
            for (String source : DOCUMENTS) {
                if (!source.endsWith(".pdf")) continue;
                File path = new File(source);
                DemoTestDocument doc = new DemoTestDocument();
                doc.setId(new DocumentId(__COLLECTION_NAME));
                doc.setName(source);
                doc.setDocState(new DemoDocState());
                doc.getDocState().setState(EEntityState.New);
                doc.setPath(path);
                doc.setUri(path.toURI().toString());
                doc.setCreatedBy("DEMO");
                doc.setModifiedBy("DEMO");
                doc.getState().setState(EEntityState.New);
                for (String d : DOCUMENTS) {
                    if (d.endsWith(".pdf")) continue;
                    File cp = new File(source);
                    DemoTestDocument cd = new DemoTestDocument();
                    cd.setId(new DocumentId(__COLLECTION_NAME));
                    cd.setName(d);
                    cd.setDocState(new DemoDocState());
                    cd.getDocState().setState(EEntityState.New);
                    cd.setPath(cp);
                    cd.setUri(cp.toURI().toString());
                    cd.setCreatedBy("DEMO");
                    cd.setModifiedBy("DEMO");
                    doc.add(cd);
                }
                doc = (DemoTestDocument) contentProvider.create(doc, userContext);
                assertNotNull(doc);
                ids.add(doc.entityKey());
            }
            Thread.sleep(10000);
            for (DocumentId id : ids) {
                DemoTestDocument doc = (DemoTestDocument) contentProvider.find(id, DemoTestDocument.class, null);
                assertNotNull(doc);
                assertEquals(DOCUMENTS.length, doc.getDocuments().size());
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}