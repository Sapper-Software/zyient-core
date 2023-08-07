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

package ai.sapper.cdc.core.messaging.kafka;

import ai.sapper.cdc.core.messaging.MessagingProcessorSettings;
import ai.sapper.cdc.core.processing.BatchMessageProcessor;
import ai.sapper.cdc.core.processing.ProcessingState;
import ai.sapper.cdc.core.state.Offset;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public abstract class KafkaBatchMessageProcessor<T, E extends Enum<?>, O extends Offset, M>
        extends BatchMessageProcessor<T, String, M, E, O, KafkaOffset> {

    protected KafkaBatchMessageProcessor(@NonNull Class<? extends ProcessingState<E, O>> stateType,
                                         @NonNull Class<? extends MessagingProcessorSettings> settingsType) {
        super(stateType, settingsType);
    }

    protected KafkaBatchMessageProcessor(@NonNull Class<? extends ProcessingState<E, O>> stateType) {
        super(stateType);
    }
}
