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

package io.zyient.core.mapping.transformers;

import com.google.common.base.Strings;
import io.zyient.base.common.utils.CommonUtils;
import io.zyient.core.mapping.DataException;
import io.zyient.core.mapping.mapper.MappingSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

@Getter
@Setter
@Accessors(fluent = true)
public class DateTransformer extends DeSerializer<Date> {
    public static final String LOCALE_OVERRIDE = "formatter.date.locale";
    public static final String FORMAT_OVERRIDE = "formatter.date.format";
    private String format;
    private Locale locale;

    public DateTransformer() {
        super(Date.class);
    }

    @Override
    public DeSerializer<Date> configure(@NonNull MappingSettings settings) throws ConfigurationException {
        name = Date.class.getCanonicalName();
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
        return this;
    }

    @Override
    public DeSerializer<Date> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        name = Date.class.getCanonicalName();
        String s = xmlConfig.getString(FORMAT_OVERRIDE);
        if (!Strings.isNullOrEmpty(s)) {
            format = s;
        } else {
            if (Strings.isNullOrEmpty(format)) {
                throw new ConfigurationException("Date format not specified...");
            }
        }
        s = xmlConfig.getString(LOCALE_OVERRIDE);
        if (!Strings.isNullOrEmpty(s)) {
            try {
                locale = CommonUtils.parseLocale(s);
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        } else {
            if (locale == null) {
                throw new ConfigurationException("Locale not specified...");
            }
        }
        return this;
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
}
