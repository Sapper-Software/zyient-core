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

package io.zyient.core.mapping.readers.impl.json;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.env.DemoDataStoreEnv;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.settings.JsonReaderSettings;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class JsonInputReaderTest {
    private static final String FILE_JSON_OBJECT = "src/test/resources/data/customers_202311231644.json";
    private static final String FILE_JSON_ARRAY = "src/test/resources/data/employees_202311232000.json";
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
    void nextBatchArray() {
        try {
            File file = new File(FILE_JSON_ARRAY);
            InputContentInfo ci = new InputContentInfo()
                    .path(file)
                    .sourceURI(file.toURI());
            JsonReaderSettings settings = new JsonReaderSettings();
            settings.setArray(true);
            settings.setBasePath("customers");
            JsonInputReader reader = (JsonInputReader) new JsonInputReader()
                    .contentInfo(ci)
                    .settings(settings);
            try (ReadCursor cursor = reader.open(env)) {
                int count = 0;
                while (true) {
                    Map<String, Object> data = cursor.next();
                    if (data == null) {
                        break;
                    }
                    count++;
                }
                assertEquals(300024, count);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void nextBatchRoot() {
        try {
            File file = new File(FILE_JSON_OBJECT);
            InputContentInfo ci = new InputContentInfo()
                    .path(file)
                    .sourceURI(file.toURI());
            JsonReaderSettings settings = new JsonReaderSettings();
            settings.setArray(false);
            settings.setBasePath("customers");
            JsonInputReader reader = (JsonInputReader) new JsonInputReader()
                    .contentInfo(ci)
                    .settings(settings);
            try (ReadCursor cursor = reader.open(env)) {
                int count = 0;
                while (true) {
                    Map<String, Object> data = cursor.next();
                    if (data == null) {
                        break;
                    }
                    count++;
                }
                assertEquals(215, count);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}