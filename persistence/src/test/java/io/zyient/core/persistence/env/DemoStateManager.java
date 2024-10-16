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

package io.zyient.core.persistence.env;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.model.Heartbeat;
import io.zyient.base.core.state.BaseStateManager;
import io.zyient.base.core.state.BaseStateManagerSettings;
import io.zyient.base.core.state.StateManagerError;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class DemoStateManager extends BaseStateManager {
    private final DemoState state = new DemoState();

    @Override
    public BaseStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                 @NonNull BaseEnv<?> env) throws StateManagerError {
        try {
            super.init(xmlConfig,
                    BaseStateManagerSettings.__CONFIG_PATH,
                    env,
                    BaseStateManagerSettings.class);
            state.setState(EDemoState.Available);
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            state.error(ex);
            throw new StateManagerError(ex);
        }
    }

    @Override
    public Heartbeat heartbeat(@NonNull String instance) throws StateManagerError {
        try {
            return heartbeat(instance, getClass(), state);
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }
}
