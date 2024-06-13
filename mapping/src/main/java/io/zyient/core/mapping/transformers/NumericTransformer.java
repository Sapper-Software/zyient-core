/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.CommonUtils;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.core.mapping.DataException;
import io.zyient.core.mapping.mapper.MappingSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class NumericTransformer<T> extends DeSerializer<T> implements PrimitiveTransformer<T> {
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

    @Override
    public DeSerializer<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        String l = xmlConfig.getString(LOCALE_OVERRIDE);
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
            if (value.startsWith("+")) {
                value = value.substring(1);
            }
            value = value.replaceAll("\\+", "");
            if (StringUtils.isBlank(value)) {
                return null;
            }
            return format.parse(value);
        } catch (Exception ex) {
            throw new DataException(ex);
        }
    }

    @Override
    public String serialize(@NonNull T value) throws DataException {
        return String.valueOf(value);
    }

    @Override
    public T getNullValue(DeserializationContext ctxt) throws JsonMappingException {
        T result = null;
        try {
            Field[] fields = ReflectionHelper.getAllFields(ctxt.getParser().getCurrentValue().getClass());
            if (fields != null) {
                String fieldName = ctxt.getParser().getCurrentName();
                Optional<Field> fieldOptional =
                        Arrays.stream(fields).filter(s -> fieldName.equals(s.getName())).findFirst();

                if (fieldOptional.isPresent()) {
                    if (ReflectionHelper.isPrimitiveTypeOrClass(fieldOptional.get().getType())) {
                        result = getDefaultPrimitiveValue();
                    }
                }
            }
        } catch (IOException e) {
            DefaultLogger.warn("unable to set default value. Keeping as null");
        }
        return result;
    }

}
