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
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UnitsParser {
    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class UnitValue {
        private double value;
        private String unit;
    }

    private static final String REGEX = "(\\d*)\\s*(\\w+)";

    public static UnitValue parse(@NonNull String source) {
        if (!Strings.isNullOrEmpty(source)) {
            Pattern pattern = Pattern.compile(REGEX);
            Matcher matcher = pattern.matcher(source);
            if (matcher.matches()) {
                String v = matcher.group(1);
                String u = matcher.group(2);

                UnitValue uv = new UnitValue();
                if (!Strings.isNullOrEmpty(v)) {
                    uv.value(Double.parseDouble(v));
                }
                if (!Strings.isNullOrEmpty(u)) {
                    uv.unit(u.trim());
                }
                return uv;
            }
        }
        return null;
    }

    public static long dataSize(@NonNull UnitValue value) {
        long size = -1;
        if (Strings.isNullOrEmpty(value.unit)) {
            size = (long) value.value();
        } else if (value.unit.compareToIgnoreCase("K") == 0 ||
                value.unit.compareToIgnoreCase("KB") == 0) {
            size = (long) (value.value * 1024);
        } else if (value.unit.compareToIgnoreCase("M") == 0 ||
                value.unit.compareToIgnoreCase("MB") == 0) {
            size = (long) (value.value * 1024 * 1024);
        } else if (value.unit.compareToIgnoreCase("G") == 0 ||
                value.unit.compareToIgnoreCase("GB") == 0) {
            size = (long) (value.value * 1024 * 1024 * 1024);
        } else if (value.unit.compareToIgnoreCase("T") == 0 ||
                value.unit.compareToIgnoreCase("TB") == 0) {
            size = (long) (value.value * 1024 * 1024 * 1024);
        }
        return size;
    }
}
