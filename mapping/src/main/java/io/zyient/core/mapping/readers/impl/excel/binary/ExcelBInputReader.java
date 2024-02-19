package io.zyient.core.mapping.readers.impl.excel.binary;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.impl.excel.ExcelReadCursor;
import io.zyient.core.mapping.readers.impl.excel.ExcelSheet;
import io.zyient.core.mapping.readers.settings.ExcelReaderSettings;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.binary.XSSFBSharedStringsTable;
import org.apache.poi.xssf.binary.XSSFBSheetHandler;
import org.apache.poi.xssf.binary.XSSFBStylesTable;
import org.apache.poi.xssf.eventusermodel.XSSFBReader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class ExcelBInputReader extends InputReader {
    private List<SheetHandler> sheets = new ArrayList<>();
    private LinkedHashMap<String, SheetHandler> sheetMap = new LinkedHashMap<>();
    private FileInputStream stream;
    private int sheetIndex = 0;
    private int rowIndex = 0;
    private boolean EOF = false;
    private SheetHandler current = null;

    @Override
    protected ReadCursor doOpen() throws IOException {
        OPCPackage pkg;
        try {
            stream = new FileInputStream(contentInfo().path());
            pkg = OPCPackage.open(stream);
            XSSFBReader r = new XSSFBReader(pkg);
            XSSFBSharedStringsTable sst = new XSSFBSharedStringsTable(pkg);
            XSSFBStylesTable xssfbStylesTable = r.getXSSFBStylesTable();
            XSSFBReader.SheetIterator it = (XSSFBReader.SheetIterator) r.getSheetsData();
            while (it.hasNext()) {
                InputStream is = it.next();
                String name = it.getSheetName();
                SheetHandler sheetHandler = new SheetHandler((ExcelReaderSettings) settings());
                XSSFBSheetHandler xssfbSheetHandler = new XSSFBSheetHandler(is, xssfbStylesTable,
                        it.getXSSFBSheetComments(), sst, sheetHandler, new DataFormatter(), false);
                xssfbSheetHandler.parse();
                sheetHandler.endSheet();
                sheetMap.put(name, sheetHandler);
                sheets.add(sheetHandler);
            }

            return new ExcelReadCursor(this, settings().getReadBatchSize());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new IOException(ex);
        }
    }

    @Override
    public List<SourceMap> fetchNextBatch() throws IOException {
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
            current = sheetMap.get(sheet.getName());
            if (current == null) {
                throw new Exception(String.format("Sheet not found. [file=%s][sheet=%s]",
                        contentInfo().path().getAbsolutePath(), sheet.getName()));
            }
        } else {
            current = sheets.get(sheet.getIndex());
            if (current == null) {
                throw new Exception(String.format("Sheet not found. [file=%s][sheet=%s]",
                        contentInfo().path().getAbsolutePath(), sheet.getName()));
            }
        }
    }

    protected List<SourceMap> readFromSheet(int count, ExcelReaderSettings settings) throws Exception {
        return current.getSheetData();
    }

    @Override
    public void close() throws IOException {
        if (this.stream != null) {
            this.stream.close();
            this.stream = null;
        }
    }
}
