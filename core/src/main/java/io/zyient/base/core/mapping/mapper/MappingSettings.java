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

package io.zyient.base.core.mapping.mapper;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.Settings;
import lombok.Getter;
import lombok.Setter;

import java.text.DateFormat;
import java.util.Locale;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@ConfigPath(path = "settings")
public class MappingSettings extends Settings {
    @Config(name = "name")
    private String name;
    @Config(name = "currencyCode", required = false)
    private String currencyCode = null;
    @Config(name = "format.date", required = false)
    private String dateFormat = null;
    @Config(name = "locale", required = false)
    private String localeStr = null;
    private Locale locale = Locale.getDefault();

    public MappingSettings() {
        DateFormat fmt = DateFormat.getDateInstance(DateFormat.DEFAULT, locale);
        dateFormat = fmt.toString();
    }

    public void postLoad() throws Exception {
        if (!Strings.isNullOrEmpty(localeStr)) {
            String[] parts = localeStr.split(",");
            String language = parts[0].trim();
            String country = parts[1].trim();
            if (Strings.isNullOrEmpty(country) || Strings.isNullOrEmpty(language)) {
                throw new Exception(String.format("Invalid locale specified. [format=<country>,<language>][value=%s]",
                        localeStr));
            }
            locale = new Locale(language, country);
            DateFormat fmt = DateFormat.getDateInstance(DateFormat.DEFAULT, locale);
            dateFormat = fmt.toString();
        }
    }
}
