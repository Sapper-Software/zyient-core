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

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.cdc.core.state.OffsetStateManager;
import ai.sapper.cdc.core.state.OffsetStateManagerSettings;
import ai.sapper.cdc.core.state.StateManagerError;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class ChronicleStateManager extends OffsetStateManager<ChronicleOffset> {
    @Override
    public OffsetStateManager<ChronicleOffset> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                    @NonNull BaseEnv<?> env) throws StateManagerError {
        try {
            super.init(xmlConfig, env, ChronicleConsumerOffsetSettings.class);
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Throwable ex) {
            state().error(ex);
            DefaultLogger.stacktrace(ex);
            throw new StateManagerError(ex);
        }
    }

    @Override
    public OffsetStateManager<ChronicleOffset> init(@NonNull OffsetStateManagerSettings settings,
                                                    @NonNull BaseEnv<?> env) throws StateManagerError {
        Preconditions.checkArgument(settings instanceof ChronicleConsumerOffsetSettings);
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


    public ChronicleConsumerState get(@NonNull String queue) throws StateManagerError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(queue));
        return get(ChronicleConsumerState.OFFSET_TYPE, queue, ChronicleConsumerState.class);
    }

    public ChronicleConsumerState create(@NonNull String queue) throws StateManagerError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(queue));
        ChronicleConsumerState state = create(ChronicleConsumerState.OFFSET_TYPE, queue, ChronicleConsumerState.class);
        state.setQueue(queue);
        ChronicleOffset offset = new ChronicleOffset();
        offset.setQueue(queue);
        state.setOffset(offset);
        return update(state);
    }

    public ChronicleConsumerState update(@NonNull ChronicleConsumerState state) throws StateManagerError {
        return super.update(state);
    }
}
