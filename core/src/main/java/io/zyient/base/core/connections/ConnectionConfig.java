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

package io.zyient.base.core.connections;

import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.Settings;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class ConnectionConfig extends ConfigReader {
    private final Class<? extends Connection> connectionClass;

    public ConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                            @NonNull String path,
                            @NonNull Class<? extends ConnectionSettings> type,
                            @NonNull Class<? extends Connection> connectionClass) {
        super(config, path, type);
        this.connectionClass = connectionClass;
    }

    public ConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                            @NonNull String path,
                            @NonNull Settings settings,
                            @NonNull Class<? extends Connection> connectionClass) {
        super(config, path, settings);
        this.connectionClass = connectionClass;
    }

    @Override
    public void read() throws ConfigurationException {
        super.read();
        ConnectionSettings settings = (ConnectionSettings) settings();
        settings.withConnectionClass(connectionClass);
    }
}
