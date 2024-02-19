package io.zyient.core.mapping.readers.impl.excel.binary;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.readers.impl.excel.ExcelDateUtil;
import io.zyient.core.mapping.readers.settings.ExcelReaderSettings;
import lombok.Getter;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.usermodel.XSSFComment;

import java.util.ArrayList;
import java.util.List;


public class SheetHandler implements XSSFSheetXMLHandler.SheetContentsHandler {
    private final ExcelReaderSettings settings;

    public SheetHandler(ExcelReaderSettings settings) {
        this.settings = settings;
        this.headers = new ArrayList<>();
    }

    @Getter
    private final List<SourceMap> sheetData = new ArrayList<>();
    private SourceMap rowMap;
    private boolean readingHeader;
    private List<String> headers;
    private int rowNumber;
    private int colIndex;

    @Override
    public void startRow(int rowNum) {
        this.rowNumber = rowNum;
        rowMap = new SourceMap();
        readingHeader = rowNum == 0 && !settings.isSkipHeader();
        colIndex = 0;
    }

    @Override
    public void endRow(int rowNum) {
        this.rowNumber = rowNum;
        if (rowNum == 0) {
            if (!settings.isSkipHeader()) {
                this.sheetData.add(this.rowMap);
            }
        } else {
            this.sheetData.add(this.rowMap);
        }
    }

    @Override
    public void cell(String cellReference, String formattedValue, XSSFComment comment) {
        formattedValue = (formattedValue == null) ? "" : formattedValue;
        if (ExcelDateUtil.isDateValid(formattedValue)) {
            try {
//                CellDateFormatter cellDateFormatter = new CellDateFormatter(dateFormat);
//                formattedValue = cellDateFormatter.format(new Date(formattedValue));
                formattedValue = formattedValue;
            } catch (Exception ex) {
                DefaultLogger.stacktrace(ex);
                DefaultLogger.error("Exception occurred while formatting the date in given format ");
            }
        }

        int columnNumber = titleToNumber(extractAlpha(cellReference));
        if (settings.getHeaders() != null) {
            if (rowNumber == 0) {
                headers.add(formattedValue);
            }else{
                rowMap.put(headers.get(columnNumber - 1), formattedValue);
            }
        } else {
            String column = String.format("%s%d", settings.getColumnPrefix(), colIndex);
            rowMap.put(column, formattedValue);
        }
        colIndex++;

    }

    private String extractAlpha(String inputStr) {
        StringBuilder alphaStr = new StringBuilder();
        for (char c : inputStr.toCharArray()) {
            if (Character.isLetter(c)) {
                alphaStr.append(c);
            } else {
                break;
            }
        }
        return alphaStr.toString();
    }

    private int titleToNumber(String s) {
        int result = 0;
        for (int i = 0; i < s.length(); i++) {
            result *= 26;
            result += s.charAt(i) - 'A' + 1;
        }
        return result;
    }

}

