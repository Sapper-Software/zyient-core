package io.zyient.core.mapping.readers.impl.excel.encrypted;

import io.zyient.core.mapping.readers.impl.excel.ExcelInputReader;
import io.zyient.core.mapping.readers.settings.ProtectedExcelReaderSettings;
import io.zyient.core.mapping.readers.util.PasswordUtil;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory;

import java.io.FileInputStream;
import java.io.IOException;

public class ProtectedExcelInputReader extends ExcelInputReader {

    @Override
    protected Workbook createWorkbook(FileInputStream stream) throws IOException {
        String password = PasswordUtil.fetchPassword(((ProtectedExcelReaderSettings) settings()).getDecryptionSecret());
        XSSFWorkbookFactory xssfWorkbookFactory = new XSSFWorkbookFactory();
        return xssfWorkbookFactory.create(stream, password);
    }
}
