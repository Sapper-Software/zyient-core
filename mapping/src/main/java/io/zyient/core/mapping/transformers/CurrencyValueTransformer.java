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

import com.google.common.base.Strings;
import io.zyient.base.common.utils.CommonUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.core.mapping.DataException;
import io.zyient.core.mapping.mapper.MappingSettings;
import io.zyient.core.mapping.model.CurrencyValue;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.text.DecimalFormat;
import java.util.Currency;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
@Accessors(fluent = true)
public class CurrencyValueTransformer extends DeSerializer<CurrencyValue> {
    public static final String LOCALE_OVERRIDE = "formatter.currency.locale";
    public static final String CURRENCY_OVERRIDE = "formatter.currency.code";
    public static final String CURRENCY_PARSE_REGEX = "%s\\s*(.*)";
    private static Pattern CURRENCY_PARSE_CODE;
    private static Pattern CURRENCY_PARSE_SYMBOL;

    private Locale locale;
    private Currency currency;
    private DecimalFormat format;

    public CurrencyValueTransformer() {
        super(CurrencyValue.class);
    }


    @Override
    public DeSerializer<CurrencyValue> configure(@NonNull MappingSettings settings) throws ConfigurationException {
        name = CurrencyValue.class.getCanonicalName();
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
        format = (DecimalFormat) DecimalFormat.getInstance(locale);
        currency = Currency.getInstance(locale);
        String c = settings.getCurrencyCode();
        if (!Strings.isNullOrEmpty(c)) {
            currency = Currency.getInstance(c);
        }
        CURRENCY_PARSE_SYMBOL = Pattern.compile(String.format(CURRENCY_PARSE_REGEX, currency.getSymbol()));
        CURRENCY_PARSE_CODE = Pattern.compile(String.format(CURRENCY_PARSE_REGEX, currency.getCurrencyCode()));
        return this;
    }

    @Override
    public DeSerializer<CurrencyValue> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        name = CurrencyValue.class.getCanonicalName();
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
        format = (DecimalFormat) DecimalFormat.getInstance(locale);
        currency = Currency.getInstance(locale);
        String c = xmlConfig.getString(CURRENCY_OVERRIDE);
        if (!Strings.isNullOrEmpty(c)) {
            currency = Currency.getInstance(c);
        }
        CURRENCY_PARSE_SYMBOL = Pattern.compile(String.format(CURRENCY_PARSE_REGEX, currency.getSymbol()));
        CURRENCY_PARSE_CODE = Pattern.compile(String.format(CURRENCY_PARSE_REGEX, currency.getCurrencyCode()));
        return this;
    }

    @Override
    public CurrencyValue transform(@NonNull Object source) throws DataException {
        if (source instanceof CurrencyValue) {
            return (CurrencyValue) source;
        } else if (ReflectionHelper.isNumericType(source.getClass())) {
            double dv = (double) source;
            return new CurrencyValue(currency, dv);
        } else if (source instanceof String value) {
            try {
                Matcher m = CURRENCY_PARSE_SYMBOL.matcher(value);
                if (m.matches()) {
                    String ds = m.group(1);
                    if (!Strings.isNullOrEmpty(ds)) {
                        Number number = format.parse(ds);
                        return new CurrencyValue(currency, number.doubleValue());
                    }
                }
                m = CURRENCY_PARSE_CODE.matcher(value);
                if (m.matches()) {
                    String ds = m.group(1);
                    if (!Strings.isNullOrEmpty(ds)) {
                        Number number = format.parse(ds);
                        return new CurrencyValue(currency, number.doubleValue());
                    }
                }
                Number number = format.parse(value);
                return new CurrencyValue(currency, number.doubleValue());
            } catch (Exception ex) {
                throw new DataException(ex);
            }
        }
        throw new DataException(String.format("Cannot transform to Currency. [source=%s]", source.getClass()));
    }

    @Override
    public String serialize(@NonNull CurrencyValue value) throws DataException {
        return String.format("%s %f", currency.getCurrencyCode(), value.getValue());
    }
}
