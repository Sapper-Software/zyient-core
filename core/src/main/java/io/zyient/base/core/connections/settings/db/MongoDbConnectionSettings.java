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

package io.zyient.base.core.connections.settings.db;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * <pre>
 *     <connections>
 *         <connection>
 *             <class>[Connection class]</class>
 *             <mongodb>
 *                  <name>[Connection name, must be unique]</name>
 *                  <host>[MongoDB Host (name or IP)</host>
 *                  <port>[MongoDB port, default = 27017]</port>
 *                  <user>[DB User name]</user>
 *                  <passwordKey>[Password Key in the KeyStore]</passwordKey>
 *                  <db>[Collection name]</db>
 *                  <poolSize>[Connection Pool Size, default = 32]</poolSize>
 *             </mongodb>
 *         </connection>
 *     </connections>
 *     ...
 *     <save>[Save connections to ZooKeeper, default=false]</save>
 *     <override>[Override saved connections, default = true]</override>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@ConfigPath(path = "mongodb")
public class MongoDbConnectionSettings extends ConnectionSettings {
    public static class Constants {
        public static final String CONFIG_HOST = "host";
        public static final String CONFIG_PORT = "port";
        public static final String CONFIG_DB = "db";
    }

    @Config(name = JdbcConnectionSettings.Constants.CONFIG_USER)
    private String user;
    @Config(name = Constants.CONFIG_HOST)
    private String host;
    @Config(name = Constants.CONFIG_PORT, required = false, type = Integer.class)
    private int port = 27017;
    @Config(name = Constants.CONFIG_DB)
    private String db;
    @Config(name = JdbcConnectionSettings.Constants.CONFIG_PASS_KEY)
    private String password;
    @Config(name = JdbcConnectionSettings.Constants.CONFIG_POOL_SIZE, required = false, type = Integer.class)
    private int poolSize = 32;

    public MongoDbConnectionSettings() {
        setType(EConnectionType.db);
    }

    public MongoDbConnectionSettings(@NonNull ConnectionSettings settings) {
        super(settings);
        Preconditions.checkArgument(settings instanceof MongoDbConnectionSettings);
        this.user = ((MongoDbConnectionSettings) settings).getUser();
        this.host = ((MongoDbConnectionSettings) settings).getHost();
        this.port = ((MongoDbConnectionSettings) settings).getPort();
        this.db = ((MongoDbConnectionSettings) settings).getDb();
        this.password = ((MongoDbConnectionSettings) settings).password;
        this.poolSize = ((MongoDbConnectionSettings) settings).getPoolSize();
    }
}
