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

package io.zyient.core.filesystem.sync;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.model.Heartbeat;
import io.zyient.base.core.processing.ProcessStateManager;
import io.zyient.base.core.processing.ProcessingState;
import io.zyient.base.core.state.BaseStateManager;
import io.zyient.base.core.state.BaseStateManagerSettings;
import io.zyient.base.core.state.Offset;
import io.zyient.base.core.state.StateManagerError;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class SyncEventStateManager<O extends Offset> extends ProcessStateManager<EEventProcessorState, O> {


    protected SyncEventStateManager(@NonNull Class<? extends ProcessingState<EEventProcessorState, O>> processingStateType) {
        super(processingStateType);
    }

    @Override
    public BaseStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                 @NonNull BaseEnv<?> env) throws StateManagerError {
        try {
            super.init(xmlConfig, BaseStateManagerSettings.__CONFIG_PATH, env, BaseStateManagerSettings.class);
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new StateManagerError(ex);
        }
    }

    @Override
    public Heartbeat heartbeat(@NonNull String instance) throws StateManagerError {
        try {
            ProcessingState<EEventProcessorState, O> state = null;
            for (ProcessingState<EEventProcessorState, O> ps : processingStates().values()) {
                if (state == null) {
                    state = ps;
                } else if (ps.hasError()) {
                    state = ps;
                    break;
                }
            }
            if (state == null) {
                throw new Exception("No valid states found...");
            }
            return heartbeat(instance, getClass(), state);
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }
}
