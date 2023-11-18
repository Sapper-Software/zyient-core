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

package io.zyient.base.core.mapping.transformers;

import com.google.common.base.Strings;
import io.zyient.base.common.utils.CommonUtils;
import io.zyient.base.core.mapping.DataException;
import io.zyient.base.core.mapping.MappingSettings;
import io.zyient.base.core.mapping.Transformer;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

@Getter
@Setter
@Accessors(fluent = true)
public class DateTransformer implements Transformer<Date> {
    public static final String LOCALE_OVERRIDE = "formatter.date.locale";
    public static final String FORMAT_OVERRIDE = "formatter.date.format";
    private String format;
    private Locale locale;

    @Override
    public String name() {
        return Date.class.getCanonicalName();
    }

    @Override
    public Transformer<Date> configure(@NonNull MappingSettings settings) throws ConfigurationException {
        String l = settings.getParameter(LOCALE_OVERRIDE);
        if (!Strings.isNullOrEmpty(l)) {
            try {
                locale = CommonUtils.parseLocale(l);
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        } else {
            if (locale == null) {
                throw new ConfigurationException("Locale not specified...");
            }
        }
        String f = settings.getParameter(FORMAT_OVERRIDE);
        if (!Strings.isNullOrEmpty(f)) {
            format = f;
        } else {
            if (Strings.isNullOrEmpty(format)) {
                throw new ConfigurationException("Date format not specified...");
            }
        }
        return null;
    }

    @Override
    public Date transform(@NonNull Object source) throws DataException {
        try {
            if (source instanceof Date) {
                return (Date) source;
            } else if (source instanceof String value) {
                SimpleDateFormat df = new SimpleDateFormat(format, locale);
                return df.parse(value);
            }
            throw new DataException(String.format("Cannot transform to Date. [source=%s]", source.getClass()));
        } catch (Exception ex) {
            throw new DataException(ex);
        }
    }

    @Override
    public String write(@NonNull Date source) throws DataException {
        SimpleDateFormat df = new SimpleDateFormat(format, locale);
        return df.format(source);
    }
}
