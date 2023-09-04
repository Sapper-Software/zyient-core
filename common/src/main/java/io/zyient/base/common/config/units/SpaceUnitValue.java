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

package io.zyient.base.common.config.units;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class SpaceUnitValue extends UnitValue<SpaceUnitValue.SpaceUnit, Long> {
    public static final String REGEX = "\\s*(\\d+)\\s*(\\w*)";
    public static final Pattern PATTERN = Pattern.compile(REGEX);

    public SpaceUnitValue() {
        setValue(0.0);
        setUnit(null);
    }

    public SpaceUnitValue(long value, @NonNull SpaceUnit unit) {
        setValue(value);
        setUnit(unit);
    }

    @Override
    public SpaceUnitValue parse(@NonNull String input) throws IllegalArgumentException {
        Matcher m = PATTERN.matcher(input);
        if (m.matches()) {
            if (m.groupCount() == 1) {
                setUnit(SpaceUnit.BYTES);
                setValue(Long.parseLong(m.group(1)));
            } else if (m.groupCount() > 1) {
                String u = m.group(2).toUpperCase();
                if (u.equals("B")) {
                    setUnit(SpaceUnit.BYTES);
                } else if (u.equals("KB") || u.equals("K")) {
                    setUnit(SpaceUnit.KILOBYTES);
                } else if (u.equals("MB") || u.equals("M")) {
                    setUnit(SpaceUnit.MEGABYTES);
                } else if (u.equals("GB") || u.equals("G")) {
                    setUnit(SpaceUnit.GIGABYTES);
                } else if (u.equals("TB") || u.equals("T")) {
                    setUnit(SpaceUnit.TERABYTES);
                } else if (u.equals("PB") || u.equals("P")) {
                    setUnit(SpaceUnit.PETABYTES);
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
            case BYTES -> {
                return (long) getValue();
            }
            case KILOBYTES -> {
                return (long) (getValue() * SpaceUnit.KILOBYTES.scale);
            }
            case MEGABYTES -> {
                return (long) (getValue() * SpaceUnit.MEGABYTES.scale);
            }
            case GIGABYTES -> {
                return (long) (getValue() * SpaceUnit.GIGABYTES.scale);
            }
            case TERABYTES -> {
                return (long) (getValue() * SpaceUnit.TERABYTES.scale);
            }
            case PETABYTES -> {
                return (long) (getValue() * SpaceUnit.PETABYTES.scale);
            }
        }
        throw new Exception(String.format("Failed to convert [%f][%s]", getValue(), getUnit().name()));
    }

    @Override
    public String toString() {
        if (getUnit() != null) {
            String u = switch (getUnit()) {
                case BYTES -> "B";
                case KILOBYTES -> "KB";
                case MEGABYTES -> "MB";
                case GIGABYTES -> "GB";
                case TERABYTES -> "TB";
                case PETABYTES -> "PB";
            };
            return String.format("%d%s", (long) getValue(), u);
        }
        return null;
    }

    public enum SpaceUnit {
        BYTES(1L),
        KILOBYTES(1024),
        MEGABYTES(1024 * 1024),
        GIGABYTES(1024L * 1024 * 1024),
        TERABYTES(1024L * 1024 * 1024 * 1024),
        PETABYTES(1024L * 1024 * 1024 * 1024 * 1024);

        static final long SCALE_BYTES = 1L;
        static final long SCALE_KILOBYTES = 1024L;
        static final long SCALE_MEGABYTES = SCALE_KILOBYTES * 1024;
        static final long SCALE_GIGABYTES = SCALE_MEGABYTES * 1024;
        static final long SCALE_TERABYTES = SCALE_GIGABYTES * 1024;
        static final long SCALE_PETABYTES = SCALE_TERABYTES * 1024;

        private long scale;

        SpaceUnit(long scale) {
            this.scale = scale;
        }

        public long scale() {
            return scale;
        }
    }
}
