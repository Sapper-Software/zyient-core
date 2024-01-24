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

package io.zyient.core.messaging.processing;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.core.processing.ProcessingState;
import io.zyient.base.core.state.Offset;
import io.zyient.core.messaging.ReceiverOffset;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class MessageProcessorState<E extends Enum<?>, O extends Offset, M extends ReceiverOffset> extends ProcessingState<E, O> {
    private M messageOffset;

    public MessageProcessorState(@NonNull E errorState,
                                 @NonNull E initState,
                                 @NonNull String type) {
        super(errorState, initState, type);
    }

    public MessageProcessorState(@NonNull MessageProcessorState<E, O, M> state) {
        super(state);
        messageOffset = state.getMessageOffset();
    }
}
