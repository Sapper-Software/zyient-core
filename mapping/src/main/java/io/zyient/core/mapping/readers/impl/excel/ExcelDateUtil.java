package io.zyient.core.mapping.readers.impl.excel;

public class ExcelDateUtil {
    public static boolean isDateValid(String dateStr) {
        dateStr = dateStr.replace("\\", "");
        String[] dateFormats = {
                "yyyy-MM-dd", "MM/dd/yyyy", "dd-MM-yyyy", "yyyy/MM/dd", "M/dd/yy", "MM/dd/yy", "MM/d/yy", "M/d/yy", "M/d/yyyy"
        };
        for (String format : dateFormats) {
            if (format.equalsIgnoreCase(dateStr)) {
                return true;
            }
        }
        return false;
    }
}
