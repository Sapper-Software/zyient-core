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

package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.AbstractEnvState;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.BaseEnvSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class DemoEnv extends BaseEnv<DemoEnv.EDemoState> {
    public DemoEnv() {
        super("demo");
    }

    public enum EDemoState {
        Error, Available, Stopped
    }

    public static class DemoState extends AbstractEnvState<EDemoState> {

        public DemoState() {
            super(EDemoState.Error, EDemoState.Available);
        }

        @Override
        public boolean isAvailable() {
            return getState() == EDemoState.Available;
        }

        @Override
        public boolean isTerminated() {
            return (getState() == EDemoState.Stopped || hasError());
        }
    }

    public static final String __CONFIG_PATH = "demo";
    private static final String CONFIG_CONNECTIONS = "connections.path";
    private static final String TEST_PASSWD = "test1234";

    private HierarchicalConfiguration<ImmutableNode> configNode;
    private final String module = "TEST";

    @Getter
    @Setter
    public static class DemoEnvSettings extends BaseEnvSettings {

    }

    public BaseEnv<EDemoState> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        withStoreKey(TEST_PASSWD);
        super.init(xmlConfig, new DemoState(), DemoEnvSettings.class);

        configNode = baseConfig().configurationAt(__CONFIG_PATH);

        return this;
    }
}
