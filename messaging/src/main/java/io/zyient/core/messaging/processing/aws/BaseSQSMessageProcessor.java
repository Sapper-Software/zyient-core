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

package io.zyient.core.messaging.processing.aws;

import io.zyient.base.core.processing.ProcessingState;
import io.zyient.base.core.state.Offset;
import io.zyient.core.messaging.MessagingProcessorSettings;
import lombok.NonNull;

public abstract class BaseSQSMessageProcessor<E extends Enum<?>, O extends Offset, M>
        extends SQSMessageProcessor<String, E, O, M> {
    protected BaseSQSMessageProcessor(@NonNull Class<? extends ProcessingState<E, O>> stateType,
                                      @NonNull Class<? extends MessagingProcessorSettings> settingsType) {
        super(stateType, settingsType);
    }

    protected BaseSQSMessageProcessor(@NonNull Class<? extends ProcessingState<E, O>> stateType) {
        super(stateType);
    }
}
