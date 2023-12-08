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

package io.zyient.core.messaging.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.state.OffsetStateManager;
import io.zyient.base.core.state.OffsetStateManagerSettings;
import io.zyient.base.core.state.StateManagerError;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class KafkaStateManager extends OffsetStateManager<KafkaOffset> {

    @Override
    public OffsetStateManager<KafkaOffset> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                @NonNull BaseEnv<?> env) throws StateManagerError {
        try {
            super.init(xmlConfig, env, KafkaConsumerOffsetSettings.class);
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Throwable ex) {
            state().error(ex);
            DefaultLogger.stacktrace(ex);
            throw new StateManagerError(ex);
        }
    }

    @Override
    public OffsetStateManager<KafkaOffset> init(@NonNull OffsetStateManagerSettings settings,
                                                @NonNull BaseEnv<?> env) throws StateManagerError {
        Preconditions.checkArgument(settings instanceof KafkaConsumerOffsetSettings);
        try {
            setup(settings, env);
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Throwable ex) {
            state().error(ex);
            DefaultLogger.stacktrace(ex);
            throw new StateManagerError(ex);
        }
    }

    public KafkaConsumerState get(@NonNull String topic, int partition) throws StateManagerError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));
        Preconditions.checkArgument(partition >= 0);
        String name = String.format("%s/%d", topic, partition);
        return get(KafkaConsumerState.OFFSET_TYPE, name, KafkaConsumerState.class);
    }

    public KafkaConsumerState create(@NonNull String topic, int partition) throws StateManagerError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));
        Preconditions.checkArgument(partition >= 0);
        String name = String.format("%s/%d", topic, partition);
        KafkaConsumerState state = create(KafkaConsumerState.OFFSET_TYPE, name, KafkaConsumerState.class);
        state.setTopic(topic);
        state.setPartition(partition);
        KafkaOffset offset = new KafkaOffset();
        offset.setTopic(topic);
        offset.setPartition(partition);
        state.setOffset(offset);

        return update(state);
    }

    public KafkaConsumerState update(@NonNull KafkaConsumerState offset) throws StateManagerError {
        return super.update(offset);
    }
}
