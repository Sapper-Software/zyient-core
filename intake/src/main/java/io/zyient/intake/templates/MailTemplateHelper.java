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

package io.zyient.intake.templates;

import com.google.common.base.Strings;
import io.zyient.intake.model.EmailJson;
import io.zyient.intake.utils.DateUtils;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

public class MailTemplateHelper {
    public static final String VAR_PREFIX = "m";
    public static final String VAR_SENDER_NAME = "sender_name";
    public static final String VAR_SENDER_EMAIL = "sender_email";
    public static final String VAR_DATE = "date";
    public static final String VAR_SUBJECT = "subject";
    public static final String VAR_SIGN = "signature";

    public static Map<String, String> generateContext(@Nonnull EmailJson email,
                                                      String signature,
                                                      String dateFormat) {
        Map<String, String> map = new HashMap<>();
        map.put(getVar(VAR_SENDER_NAME), email.getSenderName());
        map.put(getVar(VAR_SENDER_EMAIL), email.getSenderEmail().toString());
        if (!Strings.isNullOrEmpty(dateFormat))
            map.put(getVar(VAR_DATE), DateUtils.formatTimestamp(dateFormat));
        else
            map.put(getVar(VAR_DATE), DateUtils.formatTimestamp());
        map.put(getVar(VAR_SUBJECT), email.getHeader().getSubject());
        if (!Strings.isNullOrEmpty(signature)) {
            map.put(getVar(VAR_SIGN), signature);
        }
        return map;
    }

    private static String getVar(String name) {
        return String.format("%s_%s", VAR_PREFIX, name);
    }
}
