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

package io.zyient.core.mapping.readers.impl.separated;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.settings.FlattenedInputReaderSettings;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class FlattenedInputReaderTest {
    private static final String FILE_WITHOUT_HEADER = "src/test/resources/data/TransactionDetail_2023-07-03.txt";

    @Test
    void nextBatchWitCustomHeader() {
        try {
            File file = new File(FILE_WITHOUT_HEADER);
            InputContentInfo ci = new InputContentInfo()
                    .path(file)
                    .sourceURI(file.toURI());
            Map<String, Boolean> sectionHeaders = Map.of(
                    "Account Number", true,
                    "Account Name", true,
                    "As of date", true,
                    "Position Type", true
            );
            FlattenedInputReaderSettings settings = new FlattenedInputReaderSettings();
            settings.setHasHeader(false);
            settings.setSectionSeparator("\"\"");
            settings.setFieldSeparator(":");
            settings.setQuote("'");
            settings.setFields(sectionHeaders);
            settings.setReadBatchSize(32);
            try (FlattenedInputReader reader = (FlattenedInputReader) new FlattenedInputReader()
                    .contentInfo(ci)
                    .settings(settings)) {
                try (SeparatedReadCursor cursor = (SeparatedReadCursor) reader.open()) {
                    int count = 0;
                    while (true) {
                        Map<String, Object> data = cursor.next();
                        if (data == null) {
                            break;
                        }
                        count++;
                    }
                    assertEquals(115, count);
                }
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}