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

package io.zyient.base.core.mapping.readers.impl.separated;

import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.mapping.model.InputContentInfo;
import io.zyient.base.core.mapping.readers.settings.SeparatedReaderSettings;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SeparatedInputReaderTest {
    private static final String FILE_WITH_HEADER = "src/test/resources/data/customers_202311231439.csv";
    private static final String FILE_WITHOUT_HEADER = "src/test/resources/data/employees_202311231443.csv";

    @Test
    void nextBatchWitCustomHeader() {
        try {
            List<String> headers = List.of("ID", "DOB", "FirstName", "LastName", "Gender", "HireDate");

            File file = new File(FILE_WITHOUT_HEADER);
            InputContentInfo ci = new InputContentInfo()
                    .path(file)
                    .sourceURI(file.toURI());
            SeparatedReaderSettings settings = new SeparatedReaderSettings();
            settings.setHasHeader(false);
            settings.setHeaders(headers);
            settings.setReadBatchSize(32);
            SeparatedInputReader reader = (SeparatedInputReader) new SeparatedInputReader()
                    .contentInfo(ci)
                    .settings(settings);
            try (SeparatedReadCursor cursor = (SeparatedReadCursor) reader.open()) {
                int count = 0;
                while (true) {
                    Map<String, Object> data = cursor.next();
                    if (data == null) {
                        break;
                    }
                    String v = (String) data.get(headers.get(4));
                    assertFalse(Strings.isNullOrEmpty(v));
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
    void nextBatchWithoutHeader() {
        try {
            File file = new File(FILE_WITHOUT_HEADER);
            InputContentInfo ci = new InputContentInfo()
                    .path(file)
                    .sourceURI(file.toURI());
            SeparatedReaderSettings settings = new SeparatedReaderSettings();
            settings.setHasHeader(false);
            settings.setReadBatchSize(32);
            SeparatedInputReader reader = (SeparatedInputReader) new SeparatedInputReader()
                    .contentInfo(ci)
                    .settings(settings);
            try (SeparatedReadCursor cursor = (SeparatedReadCursor) reader.open()) {
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
    void nextBatchWithHeader() {
        try {
            File file = new File(FILE_WITH_HEADER);
            InputContentInfo ci = new InputContentInfo()
                    .path(file)
                    .sourceURI(file.toURI());
            SeparatedReaderSettings settings = new SeparatedReaderSettings();
            settings.setHasHeader(true);
            settings.setReadBatchSize(32);
            SeparatedInputReader reader = (SeparatedInputReader) new SeparatedInputReader()
                    .contentInfo(ci)
                    .settings(settings);
            try (SeparatedReadCursor cursor = (SeparatedReadCursor) reader.open()) {
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