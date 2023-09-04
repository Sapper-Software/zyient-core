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

package io.zyient.base.core.state;

import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.messaging.chronicle.ChronicleOffset;
import io.zyient.base.core.processing.ProcessStateManager;
import io.zyient.base.core.model.Heartbeat;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class DemoStateManager extends ProcessStateManager<EDemoProcessingState, ChronicleOffset> {
    public DemoStateManager() {
        super(DemoProcessingState.class);
    }

    @Override
    public BaseStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig, @NonNull BaseEnv<?> env) throws StateManagerError {
        try {
            super.init(xmlConfig,
                    BaseStateManagerSettings.__CONFIG_PATH,
                    env,
                    BaseStateManagerSettings.class);
            processingState().setState(EDemoProcessingState.Running);
            update(processingState());
            return this;
        } catch (Exception ex) {
            if (processingState() != null)
                processingState().error(ex);
            throw new StateManagerError(ex);
        }
    }

    @Override
    public Heartbeat heartbeat(@NonNull String instance) throws StateManagerError {
        try {
            return heartbeat(instance, getClass(), processingState());
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }
}
