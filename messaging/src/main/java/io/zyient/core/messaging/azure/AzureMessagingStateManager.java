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

package io.zyient.core.messaging.azure;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.state.OffsetStateManager;
import io.zyient.base.core.state.OffsetStateManagerSettings;
import io.zyient.base.core.state.StateManagerError;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class AzureMessagingStateManager extends OffsetStateManager<AzureMessageOffset> {
    @Override
    public OffsetStateManager<AzureMessageOffset> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                       @NonNull BaseEnv<?> env) throws StateManagerError {
        try {
            super.init(xmlConfig, env, AzureMessagingConsumerOffsetSettings.class);
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state().error(ex);
            DefaultLogger.stacktrace(ex);
            throw new StateManagerError(ex);
        }
    }

    @Override
    public OffsetStateManager<AzureMessageOffset> init(@NonNull OffsetStateManagerSettings settings,
                                                       @NonNull BaseEnv<?> env) throws StateManagerError {
        try {
            setup(settings, env);
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state().error(ex);
            DefaultLogger.stacktrace(ex);
            throw new StateManagerError(ex);
        }
    }

    public AzureMessagingConsumerState get(@NonNull String name) throws StateManagerError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        return get(AzureMessagingConsumerState.OFFSET_TYPE, name, AzureMessagingConsumerState.class);
    }

    public AzureMessagingConsumerState create(@NonNull String name, @NonNull String queue) throws StateManagerError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(queue));
        AzureMessagingConsumerState state
                = create(AzureMessagingConsumerState.OFFSET_TYPE, name, AzureMessagingConsumerState.class);
        state.setQueue(queue);
        AzureMessageOffset offset = new AzureMessageOffset();
        offset.setQueue(queue);
        offset.setOffsetRead(new AzureMessageOffsetValue());
        offset.setOffsetCommitted(new AzureMessageOffsetValue());
        state.setOffset(offset);
        return update(state);
    }
}
