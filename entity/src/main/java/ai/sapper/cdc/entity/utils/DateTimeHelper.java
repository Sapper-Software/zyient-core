package ai.sapper.cdc.entity.utils;

import ai.sapper.cdc.common.utils.DefaultLogger;
import lombok.NonNull;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Date;

public class DateTimeHelper {

    public static DateTime parseISO8601(@NonNull String txt) {
        try {
            return ISODateTimeFormat.dateTimeParser().parseDateTime(txt);
        } catch (RuntimeException re) {
            DefaultLogger.error(
                    String.format("Error parsing date: [value=%s][error=%s]", txt, re.getLocalizedMessage()));
            throw re;
        }
    }

    public static String toISODateString(@NonNull Date date) {
        DateTime dt = new DateTime(date.getTime());
        return dt.toString();
    }

    public static String toISODateString(@NonNull DateTime date) {
        return date.toString();
    }
}
