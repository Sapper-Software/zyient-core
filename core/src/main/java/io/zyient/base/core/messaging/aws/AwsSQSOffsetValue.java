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

package io.zyient.base.core.messaging.aws;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.core.messaging.OffsetValue;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@ToString
public class AwsSQSOffsetValue extends OffsetValue {
    private long index = 0;

    public AwsSQSOffsetValue() {

    }

    public AwsSQSOffsetValue(@NonNull AwsSQSOffsetValue source) {
        this.index = source.index;
    }

    public AwsSQSOffsetValue(long index) {
        this.index = index;
    }

    @Override
    public String asString() {
        return String.valueOf(index);
    }

    public AwsSQSOffsetValue parse(@NonNull String value) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        index = Long.parseLong(value);

        return this;
    }

    @Override
    public int compareTo(@NonNull OffsetValue offsetValue) {
        Preconditions.checkArgument(offsetValue instanceof AwsSQSOffsetValue);
        return (int) (index - ((AwsSQSOffsetValue) offsetValue).index);
    }
}
