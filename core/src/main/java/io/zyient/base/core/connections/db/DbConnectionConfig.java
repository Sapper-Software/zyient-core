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

package io.zyient.base.core.connections.db;

import io.zyient.base.core.connections.ConnectionConfig;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.db.JdbcConnectionSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
@Accessors(fluent = true)
public class DbConnectionConfig extends ConnectionConfig {
    public DbConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              @NonNull String path,
                              @NonNull Class<? extends DbConnection> connectionClass) {
        super(config, path, JdbcConnectionSettings.class, connectionClass);
    }

    public DbConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              @NonNull String path,
                              @NonNull Class<? extends ConnectionSettings> type,
                              @NonNull Class<? extends DbConnection> connectionClass) {
        super(config, path, type, connectionClass);
    }

}
