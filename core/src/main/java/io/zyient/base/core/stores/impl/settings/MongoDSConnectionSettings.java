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

package io.zyient.base.core.stores.impl.settings;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.connections.settings.JdbcConnectionSettings;
import io.zyient.base.core.connections.settings.MongoDbConnectionSettings;
import io.zyient.base.core.stores.AbstractConnectionSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@ConfigPath(path = "mongo")
public class MongoDSConnectionSettings extends AbstractConnectionSettings {

    @Config(name = JdbcConnectionSettings.Constants.CONFIG_USER)
    private String user;
    @Config(name = MongoDbConnectionSettings.Constants.CONFIG_HOST)
    private String host;
    @Config(name = MongoDbConnectionSettings.Constants.CONFIG_PORT, required = false, type = Integer.class)
    private int port = 27017;
    @Config(name = MongoDbConnectionSettings.Constants.CONFIG_DB)
    private String db;
    @Config(name = JdbcConnectionSettings.Constants.CONFIG_PASS_KEY)
    private String password;
    @Config(name = JdbcConnectionSettings.Constants.CONFIG_POOL_SIZE, required = false, type = Integer.class)
    private int poolSize = 32;

    public MongoDSConnectionSettings() {
        setType(EConnectionType.db);
    }

    public MongoDSConnectionSettings(@NonNull ConnectionSettings settings) {
        super(settings);
        Preconditions.checkArgument(settings instanceof MongoDSConnectionSettings);
        this.user = ((MongoDSConnectionSettings) settings).getUser();
        this.host = ((MongoDSConnectionSettings) settings).getHost();
        this.port = ((MongoDSConnectionSettings) settings).getPort();
        this.db = ((MongoDSConnectionSettings) settings).getDb();
        this.password = ((MongoDSConnectionSettings) settings).getPassword();
        this.poolSize = ((MongoDSConnectionSettings) settings).getPoolSize();
    }
}
