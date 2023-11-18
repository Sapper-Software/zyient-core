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

package io.zyient.base.core.mapping.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.text.NumberFormat;
import java.util.Currency;
import java.util.Locale;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class CurrencyValue {
    private Currency currency;
    private Double value;

    public CurrencyValue() {

    }

    public CurrencyValue(@NonNull String code) {
        currency = Currency.getInstance(code);
    }

    public CurrencyValue(@NonNull Locale locale) {
        currency = Currency.getInstance(locale);
    }

    public CurrencyValue(@NonNull Currency currency,
                         @NonNull Double value) {
        this.currency = currency;
        this.value = value;
    }

    private String format(int precision) {
        if (value != null) {
            String fmt = "%." + precision + "f";
            return String.format(fmt, precision);
        }
        return null;
    }

    @Override
    public String toString() {
        return toString(Locale.getDefault());
    }

    public String toString(@NonNull Locale locale) {
        if (value != null && currency != null) {
            NumberFormat fmt = NumberFormat.getInstance(locale);
            String dv = fmt.format(value);
            return String.format("%s%s", currency.getSymbol(), dv);
        }
        return null;
    }
}
