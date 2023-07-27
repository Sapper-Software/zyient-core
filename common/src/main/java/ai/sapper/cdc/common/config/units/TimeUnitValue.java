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

package ai.sapper.cdc.common.config.units;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class TimeUnitValue extends UnitValue<TimeUnit, Long> {
    public static final String REGEX = "\\s*(\\d+)\\s*(\\w*)";
    public static final Pattern PATTERN = Pattern.compile(REGEX);

    public TimeUnitValue() {
        setValue(0.0);
        setUnit(null);
    }

    public TimeUnitValue(long value, @NonNull TimeUnit unit) {
        setValue(value);
        setUnit(unit);
    }

    @Override
    public TimeUnitValue parse(@NonNull String input) throws IllegalArgumentException {
        Matcher m = PATTERN.matcher(input);
        if (m.matches()) {
            if (m.groupCount() == 1) {
                setUnit(TimeUnit.MILLISECONDS);
                setValue(Long.parseLong(m.group(1)));
            } else if (m.groupCount() > 1) {
                String u = m.group(2).toUpperCase();
                if (u.equals("NS")) {
                    setUnit(TimeUnit.NANOSECONDS);
                } else if (u.equals("US")) {
                    setUnit(TimeUnit.MICROSECONDS);
                } else if (u.equals("MS")) {
                    setUnit(TimeUnit.MILLISECONDS);
                } else if (u.equals("S")) {
                    setUnit(TimeUnit.SECONDS);
                } else if (u.equals("M")) {
                    setUnit(TimeUnit.MINUTES);
                } else if (u.equals("H")) {
                    setUnit(TimeUnit.HOURS);
                } else if (u.equals("D")) {
                    setUnit(TimeUnit.DAYS);
                }
                setValue(Long.parseLong(m.group(1)));
            }
            return this;
        }
        throw new IllegalArgumentException(String.format("Failed to parse time: [%s]", input));
    }

    @Override
    public Long normalized() throws Exception {
        if (getUnit() == null) {
            throw new Exception("No value parsed...");
        }
        switch (getUnit()) {
            case MILLISECONDS -> {
                return (long) getValue();
            }
            case NANOSECONDS -> {
                return TimeUnit.NANOSECONDS.toMillis((long) getValue());
            }
            case MICROSECONDS -> {
                return TimeUnit.MICROSECONDS.toMillis((long) getValue());
            }
            case SECONDS -> {
                return TimeUnit.SECONDS.toMillis((long) getValue());
            }
            case MINUTES -> {
                return TimeUnit.MINUTES.toMillis((long) getValue());
            }
            case DAYS -> {
                return TimeUnit.DAYS.toMillis((long) getValue());
            }
            case HOURS -> {
                return TimeUnit.HOURS.toMillis((long) getValue());
            }
        }
        throw new Exception(String.format("Failed to convert [%f][%s]", getValue(), getUnit().name()));
    }

    @Override
    public String toString() {
        if (getUnit() != null) {
            String u = switch (getUnit()) {
                case NANOSECONDS -> "NS";
                case MICROSECONDS -> "US";
                case MILLISECONDS -> "MS";
                case SECONDS -> "S";
                case MINUTES -> "M";
                case HOURS -> "H";
                case DAYS -> "D";
            };
            return String.format("%d%s", (long) getValue(), u);
        }
        return null;
    }
}
