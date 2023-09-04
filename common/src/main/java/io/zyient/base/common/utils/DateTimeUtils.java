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

package io.zyient.base.common.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.GlobalConstants;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nonnull;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to parse/write datetime.
 */
public class DateTimeUtils {
    /**
     * Parse the Date/Time string using the specified format string.
     *
     * @param datetime - Date/Time string.
     * @param format   - Format string.
     * @return - Parsed DateTime.
     */
    public static DateTime parse(String datetime, String format) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(datetime));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(format));

        DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
        return formatter.parseDateTime(datetime);
    }

    /**
     * Parse the Date string using the default Date Format.
     *
     * @param date - Date String
     * @return - Parsed DateTime.
     */
    public static DateTime parseDateTime(String date) {
        return parse(date, GlobalConstants.DEFAULT_JODA_DATE_FORMAT);
    }

    /**
     * Parse the Date/Time string using the default Date/Time Format.
     *
     * @param datetime - Date/Time string.
     * @return - Parsed DateTime.
     */
    public static DateTime parse(String datetime) {
        return parse(datetime, GlobalConstants.DEFAULT_JODA_DATETIME_FORMAT);
    }

    /**
     * Get the Date string for the specified DateTime. Uses the default Date Format.
     *
     * @param dateTime - DateTime to stringify.
     * @return - String Date.
     */
    public static String toDateString(DateTime dateTime) {
        Preconditions.checkArgument(dateTime != null);
        return dateTime.toString(GlobalConstants.DEFAULT_JODA_DATE_FORMAT);
    }

    /**
     * Get the Date/Time string for the specified DateTime. Uses the default Date/Time Format.
     *
     * @param dateTime - DateTime to stringify.
     * @return - String Date/Time.
     */
    public static String toString(DateTime dateTime) {
        Preconditions.checkArgument(dateTime != null);
        return dateTime.toString(GlobalConstants.DEFAULT_JODA_DATETIME_FORMAT);
    }

    /**
     * Get the Date/Time string for the specified DateTime. Uses the default Date/Time Format.
     *
     * @param timestamp - Timestamp to stringify.
     * @return - String Date/Time.
     */
    public static String toString(long timestamp) {
        DateTime dateTime = new DateTime(timestamp);
        return dateTime.toString(GlobalConstants.DEFAULT_JODA_DATETIME_FORMAT);
    }

    /**
     * Convert to a milliseconds period.
     *
     * @param timeout - Timeout value
     * @param unit    - Time unit.
     * @return - Milliseconds window.
     */
    public static long period(long timeout, @Nonnull TimeUnit unit) {
        switch (unit) {
            case SECONDS:
                return timeout * 1000;
            case MILLISECONDS:
                return timeout;
            case DAYS:
                return timeout * 24 * 60 * 60 * 1000;
            case HOURS:
                return timeout * 60 * 60 * 1000;
            case MINUTES:
                return timeout * 60 * 1000;
        }
        return timeout;
    }

    public static final String FORMAT_PREFIX = "fmt#";
    public static final String DEFAULT_DATE_FORMAT = "MM/dd/yyyy HH:mm:SS";
    public static final String DEFAULT_TIMESTAMP_FORMAT = "MMddyyyyHHmmSS";

    public static Date parseDate(@Nonnull String input, @Nonnull String format) throws ParseException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(input));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(format));

        input = input.trim();
        if (StringUtils.isNumeric(input)) {
            return new Date(Long.parseLong(input));
        }
        int fidx = input.indexOf(FORMAT_PREFIX);
        String dates = input;
        if (fidx > 0) {
            dates = input.substring(0, fidx);
            format = input.substring(fidx + FORMAT_PREFIX.length());
        }
        SimpleDateFormat df = new SimpleDateFormat(format);
        return df.parse(dates);
    }

    public static Date parseDate(@Nonnull String input) throws ParseException {
        return parseDate(input, DEFAULT_DATE_FORMAT);
    }

    public static String formatTimestamp(long timestamp, String format) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(format));

        SimpleDateFormat df = new SimpleDateFormat(format);
        return df.format(new Date(timestamp));
    }

    public static String formatTimestamp(String format) {
        return formatTimestamp(System.currentTimeMillis(), format);
    }

    public static String formatTimestamp() {
        return formatTimestamp(DEFAULT_TIMESTAMP_FORMAT);
    }


    public static boolean equals(Date a, Date b) {
        return a == b || a != null && b != null && compareTo(a, b) == 0;
    }

    public static int compareTo(Date a, Date b) {
        return b instanceof Timestamp && !(a instanceof Timestamp) ? (new Timestamp(a.getTime())).compareTo(b) : a.compareTo(b);
    }
}