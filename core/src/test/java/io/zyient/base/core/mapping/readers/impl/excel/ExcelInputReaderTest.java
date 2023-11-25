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

package io.zyient.base.core.mapping.readers.impl.excel;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.mapping.model.Column;
import io.zyient.base.core.mapping.model.ExcelColumn;
import io.zyient.base.core.mapping.model.InputContentInfo;
import io.zyient.base.core.mapping.readers.settings.ExcelReaderSettings;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class ExcelInputReaderTest {
    private static final String FILE_EXCEL_MULTI_SHEET = "src/test/resources/data/Financial Sample.xlsx";

    @Test
    void nextBatch() {
        try {
            File file = new File(FILE_EXCEL_MULTI_SHEET);
            InputContentInfo ci = new InputContentInfo()
                    .path(file)
                    .sourceURI(file.toURI());
            ExcelReaderSettings settings = new ExcelReaderSettings();
            settings.setHeader(ExcelHeader.HeaderOnEachSheet);
            settings.setSkipHeader(true);
            List<ExcelSheet> sheets = new ArrayList<>();
            ExcelSheet sheet = new ExcelSheet();
            sheet.setIndex(0);
            sheets.add(sheet);
            sheet = new ExcelSheet();
            sheet.setIndex(1);
            sheets.add(sheet);
            settings.setSheets(sheets);
            String line = "Segment, Country	, Product, Discount Band, Units Sold, Manufacturing Price, Sale Price, Gross Sales, Discounts, Sales, COGS, Profit, Date, Month Number, Month Name, Year";
            Map<Integer, Column> columns = createColumns(line);
            settings.setHeaders(columns);

            ExcelInputReader reader = (ExcelInputReader) new ExcelInputReader()
                    .contentInfo(ci)
                    .settings(settings);
            try (ExcelReadCursor cursor = (ExcelReadCursor) reader.open()) {
                int count = 0;
                while (true) {
                    Map<String, Object> data = cursor.next();
                    if (data == null) {
                        break;
                    }
                    count++;
                }
                assertEquals(1400, count);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    private Map<Integer, Column> createColumns(String line) {
        String[] parts = line.split(",");
        Map<Integer, Column> columns = new HashMap<>();
        for (int ii = 0; ii < parts.length; ii++) {
            ExcelColumn column = new ExcelColumn();
            column.setIndex(ii);
            column.setCellIndex(ii);
            column.setName(parts[ii].trim());
            columns.put(ii, column);
        }
        return columns;
    }
}