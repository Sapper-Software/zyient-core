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

import com.google.common.base.Strings;
import lombok.NonNull;

import java.util.Locale;

public class CommonUtils {
    public static Locale parseLocale(@NonNull String localeStr) throws Exception {
        String[] parts = localeStr.split(",");
        if (parts.length != 2) {
            throw new Exception(String.format("Invalid locale string. [string=%s]", localeStr));
        }
        String language = parts[0].trim();
        String country = parts[1].trim();
        if (Strings.isNullOrEmpty(language) || Strings.isNullOrEmpty(country)) {
            throw new Exception(String.format("Invalid locale string. [string=%s]", localeStr));
        }
        return new Locale(language, country);
    }
}
