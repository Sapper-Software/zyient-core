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

package io.zyient.core.persistence.env;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.zyient.base.common.AbstractEnvState;
import io.zyient.base.core.BaseEnv;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class DemoDataStoreEnv extends DataStoreEnv<DemoDataStoreEnv.EDemoState> {
    public DemoDataStoreEnv() {
        super("content", new DemoState());
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

    public static final String __CONFIG_PATH = "persistence";
    private static final String CONFIG_CONNECTIONS = "connections.path";
    private static final String TEST_PASSWD = "test1234";

    private HierarchicalConfiguration<ImmutableNode> demoConfig;
    private final String module = "TEST";
    private final String passKey = TEST_PASSWD;

    @Getter
    @Setter
    public static class DemoEnvSettings extends DataStoreEnvSettings {

    }

    public BaseEnv<EDemoState> create(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        if (Strings.isNullOrEmpty(storeKey()))
            withStoreKey(TEST_PASSWD);
        CompositeMeterRegistry registry = new CompositeMeterRegistry();
        BaseEnv.registry(registry);
        super.init(xmlConfig, DemoEnvSettings.class);
        demoConfig = baseConfig().configurationAt(__CONFIG_PATH);
        return this;
    }
}
