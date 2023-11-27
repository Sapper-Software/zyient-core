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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.CommonUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.mapping.DataException;
import io.zyient.base.core.mapping.mapper.MappingSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.text.NumberFormat;
import java.util.Locale;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class NumericTransformer<T> extends DeSerializer<T> {
    public static final String LOCALE_OVERRIDE = "formatter.numeric.locale";

    private final Class<T> type;
    private Locale locale;
    protected NumberFormat format;

    protected NumericTransformer(@NonNull Class<T> type) {
        super(type);
        Preconditions.checkArgument(ReflectionHelper.isNumericType(type));
        this.type = type;
    }

    @Override
    public String name() {
        return type.getCanonicalName();
    }

    @Override
    public DeSerializer<T> configure(@NonNull MappingSettings settings) throws ConfigurationException {
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
        format = NumberFormat.getInstance(locale);
        return this;
    }

    protected Number parse(@NonNull String value) throws DataException {
        try {
            return format.parse(value);
        } catch (Exception ex) {
            throw new DataException(ex);
        }
    }
}
