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

package io.zyient.core.mapping.readers.impl.positional;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.core.mapping.model.Column;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.model.PositionalColumn;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.settings.PositionalReaderSettings;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class PositionalInputReaderTest {
    private static final String TEST_DIR = "zyient/test";

    private static final Map<String, Integer> columns = Map.of(
            "ID", 48,
            "NAME", 16,
            "DATE", 48,
            "QUANTITY", 16,
            "PRICE", 18,
            "TIMESTAMP", 18
    );

    @Test
    void nextBatchNoHeader() {
        try {
            int rows = 100;
            File file = generate(rows, false);
            System.out.println(file);
            InputContentInfo ci = new InputContentInfo()
                    .path(file)
                    .sourceURI(file.toURI());
            PositionalReaderSettings settings = new PositionalReaderSettings();
            settings.setSkipHeader(false);
            Map<Integer, Column> cmap = new HashMap<>();
            int start = 0;
            cmap.put(0, new PositionalColumn("ID", 0, start, null, columns.get("ID")));
            start += columns.get("ID");
            cmap.put(1, new PositionalColumn("NAME", 0, start, null, columns.get("NAME")));
            start += columns.get("NAME");
            cmap.put(2, new PositionalColumn("DATE", 0, start, null, columns.get("DATE")));
            start += columns.get("DATE");
            cmap.put(3, new PositionalColumn("QUANTITY", 0, start, null, columns.get("QUANTITY")));
            start += columns.get("QUANTITY");
            cmap.put(4, new PositionalColumn("PRICE", 0, start, null, columns.get("PRICE")));
            start += columns.get("PRICE");
            cmap.put(5, new PositionalColumn("TIMESTAMP", 0, start, null, columns.get("TIMESTAMP")));
            start += columns.get("TIMESTAMP");
            settings.setColumns(cmap);
            PositionalInputReader reader = (PositionalInputReader) new PositionalInputReader()
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
                assertEquals(rows, count);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void nextBatch() {
        try {
            int rows = 100;
            File file = generate(rows, true);
            System.out.println(file);
            InputContentInfo ci = new InputContentInfo()
                    .path(file)
                    .sourceURI(file.toURI());
            PositionalReaderSettings settings = new PositionalReaderSettings();
            settings.setSkipHeader(true);
            settings.setValidateHeader(true);
            Map<Integer, Column> cmap = new HashMap<>();
            int start = 0;
            cmap.put(0, new PositionalColumn("ID", 0, start, null, columns.get("ID")));
            start += columns.get("ID");
            cmap.put(1, new PositionalColumn("NAME", 0, start, null, columns.get("NAME")));
            start += columns.get("NAME");
            cmap.put(2, new PositionalColumn("DATE", 0, start, null, columns.get("DATE")));
            start += columns.get("DATE");
            cmap.put(3, new PositionalColumn("QUANTITY", 0, start, null, columns.get("QUANTITY")));
            start += columns.get("QUANTITY");
            cmap.put(4, new PositionalColumn("PRICE", 0, start, null, columns.get("PRICE")));
            start += columns.get("PRICE");
            cmap.put(5, new PositionalColumn("TIMESTAMP", 0, start, null, columns.get("TIMESTAMP")));
            start += columns.get("TIMESTAMP");
            settings.setColumns(cmap);
            PositionalInputReader reader = (PositionalInputReader) new PositionalInputReader()
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
                assertEquals(rows, count);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    private File generate(int count, boolean header) throws Exception {
        File dir = PathUtils.getTempDir(TEST_DIR);
        String name = UUID.randomUUID().toString();
        File file = new File(String.format("%s/%s.dat", dir.getAbsolutePath(), name));

        try (FileOutputStream fos = new FileOutputStream(file)) {
            if (header) {
                StringBuilder builder = new StringBuilder();
                int size = columns.get("ID");
                String format = "%" + size + "s";
                builder.append(String.format(format, "ID"));
                size = columns.get("NAME");
                format = "%" + size + "s";
                builder.append(String.format(format, "NAME"));
                size = columns.get("DATE");
                format = "%" + size + "s";
                builder.append(String.format(format, "DATE"));
                size = columns.get("QUANTITY");
                format = "%" + size + "s";
                builder.append(String.format(format, "QUANTITY"));
                size = columns.get("PRICE");
                format = "%" + size + "s";
                builder.append(String.format(format, "PRICE"));
                size = columns.get("TIMESTAMP");
                format = "%" + size + "s";
                builder.append(String.format(format, "TIMESTAMP"));
                builder.append("\n");
                fos.write(builder.toString().getBytes(StandardCharsets.UTF_8));
            }
            long m = System.nanoTime();
            Random rnd = new Random(System.nanoTime());
            for (int ii = 0; ii < count; ii++) {
                StringBuilder builder = new StringBuilder();
                int size = columns.get("ID");
                String format = "%" + size + "s";
                builder.append(String.format(format, UUID.randomUUID().toString()));
                size = columns.get("NAME");
                format = "%" + size + "s";
                builder.append(String.format(format, String.format("John, Doe [%d]", ii)));
                size = columns.get("DATE");
                format = "%" + size + "s";
                builder.append(String.format(format, new Date(rnd.nextLong(100000000, m)).toString()));
                size = columns.get("QUANTITY");
                format = "%0" + size + "d";
                builder.append(String.format(format, rnd.nextInt()));
                size = columns.get("PRICE");
                format = "%0" + size + "f";
                builder.append(String.format(format, rnd.nextDouble()));
                size = columns.get("TIMESTAMP");
                format = "%0" + size + "d";
                builder.append(String.format(format, System.nanoTime()));
                builder.append("\n");
                fos.write(builder.toString().getBytes(StandardCharsets.UTF_8));
            }
        }
        return file;
    }
}