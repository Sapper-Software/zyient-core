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

package io.zyient.base.core.services.test;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.zyient.base.common.AbstractEnvState;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.ws.WebServiceClient;
import io.zyient.base.core.env.BaseEnvSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class WSTestEnv extends BaseEnv<WSTestEnv.EDemoState> {
    private static final String __CONFIG_PATH_CLIENT = "ws";

    public WSTestEnv() {
        super("demo", new DemoState());
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

    private static final String TEST_PASSWD = "test1234";

    private HierarchicalConfiguration<ImmutableNode> configNode;
    private final String module = "TEST";
    private String passKey = TEST_PASSWD;
    private WebServiceClient serviceClient;

    @Getter
    @Setter
    public static class DemoEnvSettings extends BaseEnvSettings {

    }

    public BaseEnv<EDemoState> create(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        if (Strings.isNullOrEmpty(storeKey()))
            withStoreKey(TEST_PASSWD);
        CompositeMeterRegistry registry = new CompositeMeterRegistry();
        BaseEnv.registry(registry);
        super.init(xmlConfig, DemoEnvSettings.class);

        configNode = baseConfig().configurationAt(__CONFIG_PATH);
        serviceClient = new WebServiceClient()
                .init(configNode, __CONFIG_PATH_CLIENT, connectionManager());
        return this;
    }
}
