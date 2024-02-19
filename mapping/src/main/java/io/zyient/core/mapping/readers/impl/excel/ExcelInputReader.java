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

package io.zyient.core.mapping.readers.impl.excel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.model.ExcelColumn;
import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.settings.ExcelReaderSettings;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExcelInputReader extends InputReader {
    private FileInputStream stream;
    private Workbook workbook;
    private int sheetIndex = 0;
    private int rowIndex = 0;
    private boolean EOF = false;
    private Sheet current = null;

    @Override
    public ReadCursor doOpen() throws IOException {
        try {
            stream = new FileInputStream(contentInfo().path());
            workbook = createWorkbook(stream);
            return new ExcelReadCursor(this, settings().getReadBatchSize());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new IOException(ex);
        }
    }

    protected Workbook createWorkbook(FileInputStream stream) throws IOException {
        ExcelReaderSettings excelReaderSettings = (ExcelReaderSettings) settings();
        ExcelFormat format = excelReaderSettings.getExcelFormat();
        if (format == ExcelFormat.XLS) {
            return new HSSFWorkbook(stream);
        } else {
            return new XSSFWorkbook(stream);
        }
    }


    @Override
    public List<SourceMap> fetchNextBatch() throws IOException {
        Preconditions.checkState(workbook != null);
        Preconditions.checkState(settings() instanceof ExcelReaderSettings);
        if (EOF) return null;
        try {
            if (current == null) {
                checkSheet();
            }
            List<SourceMap> response = new ArrayList<>();
            int remaining = settings().getReadBatchSize();
            while (true) {
                List<SourceMap> data = readFromSheet(remaining, (ExcelReaderSettings) settings());
                if (!data.isEmpty()) {
                    response.addAll(data);
                    remaining -= data.size();
                }
                if (remaining <= 0) break;
                if (sheetIndex < ((ExcelReaderSettings) settings()).getSheets().size() - 1) {
                    sheetIndex++;
                    rowIndex = 0;
                    checkSheet();
                } else {
                    EOF = true;
                    break;
                }
            }
            return response;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    protected void checkSheet() throws Exception {
        ExcelSheet sheet = ((ExcelReaderSettings) settings()).getSheets().get(sheetIndex);
        if (!Strings.isNullOrEmpty(sheet.getName())) {
            current = workbook.getSheet(sheet.getName());
            if (current == null) {
                throw new Exception(String.format("Sheet not found. [file=%s][sheet=%s]",
                        contentInfo().path().getAbsolutePath(), sheet.getName()));
            }
        } else {
            current = workbook.getSheetAt(sheet.getIndex());
            if (current == null) {
                throw new Exception(String.format("Sheet not found. [file=%s][sheet=%d]",
                        contentInfo().path().getAbsolutePath(), sheet.getIndex()));
            }
        }
    }

    protected List<SourceMap> readFromSheet(int count, ExcelReaderSettings settings) throws Exception {
        List<SourceMap> data = new ArrayList<>();
        while (count > 0) {
            Row row = current.getRow(rowIndex);
            if (row == null) break;
            SourceMap record = new SourceMap();
            if (settings.getHeaders() != null) {
                for (int ii : settings.getHeaders().keySet()) {
                    ExcelColumn column = (ExcelColumn) settings.getHeaders().get(ii);

                    Cell cell = row.getCell(column.getCellIndex(), getMissingCellPolicy(settings));
                    Object value = getCellValue(cell);
                    if (value != null) {
                        record.put(column.getName(), value);
                    }

                }
            } else {
                int ii = 0;
                for (Cell cell : row) {
                    String column = String.format("%s%d", settings.getColumnPrefix(), ii);
                    Object value = getCellValue(cell);
                    if (value != null) {
                        record.put(column, value);
                    }
                    ii++;
                }
            }
            if (!record.isEmpty()) {
                boolean add = true;
                if (rowIndex == 0 &&
                        settings.getHeaders() != null &&
                        settings.isSkipHeader()) {
                    if (checkHeaderRow(record, settings)) {
                        add = false;
                    }
                }
                if (add) {
                    data.add(record);
                    count--;
                }
            }
            rowIndex++;
        }
        return data;
    }

    private Row.MissingCellPolicy getMissingCellPolicy(ExcelReaderSettings settings) {
        Row.MissingCellPolicy policy = null;
        switch (settings.getCellMissing()) {
            case Null -> policy = Row.MissingCellPolicy.RETURN_BLANK_AS_NULL;
            case Blank -> policy = Row.MissingCellPolicy.CREATE_NULL_AS_BLANK;
            case Both -> policy = Row.MissingCellPolicy.RETURN_NULL_AND_BLANK;
        }
        return policy;
    }

    private Object getCellValue(Cell cell) {
        Object o = null;
        if (null != cell) {
            switch (cell.getCellType()) {
                case STRING -> o = cell.getStringCellValue().trim();
                case BOOLEAN -> o = cell.getBooleanCellValue();
                case NUMERIC -> {
                    if (cell.getCellStyle().getDataFormatString() != null &&
                            ExcelDateUtil.isDateValid(cell.getCellStyle().getDataFormatString())) {
                        return cell.toString();
                    }
                    o = cell.getNumericCellValue();
                }
            }
        }
        if (o == null) {
            o = "";
        }


        return o;
    }



    private boolean checkHeaderRow(Map<String, Object> record, ExcelReaderSettings settings) {
        for (String key : record.keySet()) {
            Object v = record.get(key);
            if (!(v instanceof String value)) {
                return false;
            }
            if (key.compareToIgnoreCase(value.trim()) != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        if (workbook != null) {
            workbook.close();
            workbook = null;
        }
        if (stream != null) {
            stream.close();
            stream = null;
        }
    }


}
