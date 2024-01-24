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

package io.zyient.core.mapping.readers.impl.xml;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.settings.XmlReaderSettings;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class XmlInputReaderTest {
    private static final String FILE_XML_OBJECT = "src/test/resources/data/customers_202311231645.xml";

    @Test
    void nextBatch() {
        try {
            File file = new File(FILE_XML_OBJECT);
            InputContentInfo ci = new InputContentInfo()
                    .path(file)
                    .sourceURI(file.toURI());
            XmlReaderSettings settings = new XmlReaderSettings();
            settings.setBasePath("DATA_RECORD");
            XmlInputReader reader = (XmlInputReader) new XmlInputReader()
                    .contentInfo(ci)
                    .settings(settings);
            try (ReadCursor cursor = reader.open()) {
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