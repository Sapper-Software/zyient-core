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

package io.zyient.core.messaging.azure;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import io.zyient.base.core.state.Offset;
import io.zyient.core.messaging.ReceiverOffset;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class AzureMessageOffset extends ReceiverOffset<AzureMessageOffsetValue> {
    private String queue;

    @Override
    public int compareTo(@NonNull Offset offset) {
        Preconditions.checkArgument(offset instanceof AzureMessageOffset);
        Preconditions.checkArgument(queue.compareTo(((AzureMessageOffset) offset).queue) == 0);
        return super.compareTo(offset);
    }

    @Override
    public AzureMessageOffsetValue parse(@NonNull String value) throws Exception {
        return new AzureMessageOffsetValue().parse(value);
    }
}
