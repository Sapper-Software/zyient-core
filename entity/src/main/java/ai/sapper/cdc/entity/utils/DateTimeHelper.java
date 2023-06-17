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

package ai.sapper.cdc.entity.utils;

import ai.sapper.cdc.common.utils.DefaultLogger;
import lombok.NonNull;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTimeHelper {

    public static String dateString(@NonNull String fmt) {
        SimpleDateFormat f = new SimpleDateFormat(fmt);
        Date dt = new Date(System.currentTimeMillis());
        return f.format(dt);
    }

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
