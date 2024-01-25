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

package io.zyient.base.core.connections.settings.common;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Exists;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.concurrent.TimeUnit;


/**
 * <pre>
 *     <connections>
 *         <connection>
 *             <class>[Connection class]</class>
 *             <zookeeper>
 *                  <name>[Connection name, must be unique]</name>
 *                  <connectionString>[ZooKeeper connection string]</connectionString>
 *                  <authenticationHandler>[ZK Authentication handler class (optional)</authenticationHandler>
 *                  <retry>
 *                      <retries>[# of retries, default = 3]</retries>
 *                      <interval>[interval between retries, default = 1sec]</interval>
 *                  </retry>
 *                  <connectionTimeout>[Connection timeout, default = ignore]</connectionTimeout>
 *                  <sessionTimeout>[Session timeout, default = ignore]</sessionTimeout>
 *                  <namespace>[Namespace (optional)</namespace>
 *             </zookeeper>
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
public class ZookeeperSettings extends ConnectionSettings {
    public static final class Constants {
        public static final String CONFIG_CONNECTION = "connectionString";
        public static final String CONFIG_AUTH_HANDLER = "authenticationHandler";
        public static final String CONFIG_RETRY = "retry";
        public static final String CONFIG_RETRY_INTERVAL = "retry.interval";
        public static final String CONFIG_RETRY_TRIES = "retry.retries";
        public static final String CONFIG_CONN_TIMEOUT = "connectionTimeout";
        public static final String CONFIG_SESSION_TIMEOUT = "sessionTimeout";
        public static final String CONFIG_NAMESPACE = "namespace";
        public static final String CONFIG_ZK_CONFIG = "zookeeperConfigFile";
    }

    @Config(name = Constants.CONFIG_CONNECTION)
    private String connectionString;
    @Config(name = Constants.CONFIG_AUTH_HANDLER, required = false)
    private String authenticationHandler;
    @Config(name = Constants.CONFIG_NAMESPACE, required = false)
    private String namespace;
    @Config(name = Constants.CONFIG_RETRY, required = false, type = Exists.class)
    private boolean retryEnabled = false;
    @Config(name = Constants.CONFIG_RETRY_INTERVAL, required = false, type = Integer.class)
    private int retryInterval = 1000;
    @Config(name = Constants.CONFIG_RETRY_TRIES, required = false, type = Integer.class)
    private int retryCount = 3;
    @Config(name = Constants.CONFIG_CONN_TIMEOUT, required = false, parser = TimeValueParser.class)
    private TimeUnitValue connectionTimeout = new TimeUnitValue(10000, TimeUnit.MILLISECONDS);
    @Config(name = Constants.CONFIG_SESSION_TIMEOUT, required = false, parser = TimeValueParser.class)
    private TimeUnitValue sessionTimeout = new TimeUnitValue(30000, TimeUnit.MILLISECONDS);

    public ZookeeperSettings() {
        setType(EConnectionType.zookeeper);
    }

    public ZookeeperSettings(@NonNull ConnectionSettings settings) {
        super(settings);
        Preconditions.checkArgument(settings instanceof ZookeeperSettings);
        this.connectionString = ((ZookeeperSettings) settings).getConnectionString();
        this.authenticationHandler = ((ZookeeperSettings) settings).getAuthenticationHandler();
        this.namespace = ((ZookeeperSettings) settings).getNamespace();
        this.retryEnabled = ((ZookeeperSettings) settings).isRetryEnabled();
        this.retryInterval = ((ZookeeperSettings) settings).getRetryInterval();
        this.retryCount = ((ZookeeperSettings) settings).getRetryCount();
        this.connectionTimeout = ((ZookeeperSettings) settings).getConnectionTimeout();
        this.sessionTimeout = ((ZookeeperSettings) settings).getSessionTimeout();
    }
}
