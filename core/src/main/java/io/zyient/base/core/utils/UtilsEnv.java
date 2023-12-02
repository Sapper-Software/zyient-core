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

package io.zyient.base.core.utils;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.zyient.base.common.AbstractEnvState;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.BaseEnvSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class UtilsEnv extends BaseEnv<UtilsEnv.EUtilsState> {
    public static final String __DEFAULT_NAME = "utils";

    public enum EUtilsState {
        Unknown, Available, Disposed, Error
    }

    public static class UtilsState extends AbstractEnvState<EUtilsState> {

        public UtilsState() {
            super(EUtilsState.Error, EUtilsState.Unknown);
        }

        @Override
        public boolean isAvailable() {
            return getState() == EUtilsState.Available;
        }

        @Override
        public boolean isTerminated() {
            return (getState() == EUtilsState.Disposed || hasError());
        }
    }

    private HierarchicalConfiguration<ImmutableNode> configNode;

    public UtilsEnv(@NonNull String name) {
        super(name);
    }


    @Getter
    @Setter
    public static class UtilsEnvSettings extends BaseEnvSettings {

    }

    public BaseEnv<EUtilsState> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                     @NonNull String password) throws ConfigurationException {
        if (Strings.isNullOrEmpty(storeKey()))
            withStoreKey(password);
        CompositeMeterRegistry registry = new CompositeMeterRegistry();
        BaseEnv.registry(registry);
        super.init(xmlConfig, new UtilsState(), UtilsEnvSettings.class);

        configNode = baseConfig().configurationAt(name());

        return this;
    }
}
