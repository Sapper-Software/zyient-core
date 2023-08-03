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

package ai.sapper.cdc.core.messaging.chronicle;

import ai.sapper.cdc.core.messaging.OffsetValue;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ChronicleOffsetValue extends OffsetValue {
    private int cycle = 0;
    private long index = 0;

    public ChronicleOffsetValue() {

    }

    public ChronicleOffsetValue(int cycle, long index) {
        this.cycle = cycle;
        this.index = index;
    }

    @Override
    public String asString() {
        return String.format("%d.%d", cycle, index);
    }

    public ChronicleOffsetValue parse(@NonNull String value) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        String[] parts = value.split("\\.");
        if (parts.length != 2) {
            throw new Exception(String.format("Parse failed: [value=%s]", value));
        }
        cycle = Integer.parseInt(parts[0]);
        index = Long.parseLong(parts[1]);

        return this;
    }

    @Override
    public int compareTo(@NonNull OffsetValue offsetValue) {
        Preconditions.checkArgument(offsetValue instanceof ChronicleOffsetValue);
        int ret = cycle - ((ChronicleOffsetValue) offsetValue).cycle;
        if (ret == 0) {
            ret = (int) (index - ((ChronicleOffsetValue) offsetValue).index);
        }
        return ret;
    }
}
