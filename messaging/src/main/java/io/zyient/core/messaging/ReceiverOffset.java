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

package io.zyient.core.messaging;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import io.zyient.base.core.state.Offset;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class ReceiverOffset<T extends OffsetValue> extends Offset {
    private T offsetRead = null;
    private T offsetCommitted = null;

    @Override
    public String asString() {
        return String.format("%s::%s", offsetRead.asString(), offsetCommitted.asString());
    }

    @Override
    public Offset fromString(@NonNull String source) throws Exception {
        String[] parts = source.split("::");
        if (parts.length < 2) {
            throw new Exception(String.format("Invalid receiver offset. [value=%s]", source));
        }
        offsetRead = parse(parts[0]);
        offsetCommitted = parse(parts[1]);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(@NonNull Offset offset) {
        Preconditions.checkArgument(offset instanceof ReceiverOffset);
        long ret = offsetCommitted.compareTo(((ReceiverOffset<T>) offset).offsetCommitted);
        if (ret == 0) {
            ret = offsetRead.compareTo(((ReceiverOffset<T>) offset).offsetRead);
        }
        return (int) ret;
    }

    public abstract T parse(@NonNull String value) throws Exception;
}
