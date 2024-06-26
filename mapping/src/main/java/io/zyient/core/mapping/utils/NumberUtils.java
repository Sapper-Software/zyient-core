package io.zyient.core.mapping.utils;

import io.zyient.base.common.utils.CommonUtils;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.model.CurrencyValue;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.Currency;
import java.util.Locale;

public class NumberUtils {
    @Getter
    @Setter
    @AllArgsConstructor
    static class NegativeFormat {
        private String prefix;
        private String suffix;
    }

    public static Double parseNumber(String value, Locale locale) {
        value = value.replaceAll("\\s+", "");
        if (StringUtils.isBlank(value)) {
            return null;
        }
        if (org.apache.commons.lang3.math.NumberUtils.isParsable(value) || CommonUtils.isScientificNotation(value)) {
            return Double.parseDouble(value);
        }
        if (value.startsWith("(") || value.endsWith(")")) {
            value = value.substring(1, value.length() - 1);
            return parseNumber(value, locale);
        }
        DecimalFormat decimalFormat = (DecimalFormat) DecimalFormat.getInstance(locale);
        try {
            Number number = decimalFormat.parse(value);
            return number.doubleValue();
        } catch (ParseException e) {
            DefaultLogger.debug("unable to parse number: " + value, e);
        }
        return null;
    }

    public static CurrencyValue parseCurrency(Currency currency, String value) throws ParseException {
        return parseCurrency(currency, value, Locale.getDefault());
    }

    public static CurrencyValue parseCurrency(Currency currency, String value, Locale locale) throws ParseException {

        NegativeFormat[] negativeFormats = new NegativeFormat[]{new NegativeFormat("%s-", ""),
                new NegativeFormat("-%s", ""), new NegativeFormat("%s(", ")"),
                new NegativeFormat("(%s", ")")};

        DecimalFormat decimalFormat = (DecimalFormat) DecimalFormat.getCurrencyInstance(locale);
        for (NegativeFormat negativeFormat : negativeFormats) {
            String[] symbols = new String[]{currency.getSymbol(), currency.getCurrencyCode()};
            for (String symbol : symbols) {
                decimalFormat.setNegativePrefix(String.format(negativeFormat.prefix, symbol));
                decimalFormat.setNegativeSuffix(negativeFormat.suffix);
                try {
                    Number number = decimalFormat.parse(value);
                    return new CurrencyValue(currency, number.doubleValue());
                } catch (ParseException e) {
                    DefaultLogger.debug("Currency format parsing failed");
                }
            }
        }

        return null;
    }

    private static String removeSpaces(String value) {
        return value.replaceAll("\\s+", "");
    }
}

