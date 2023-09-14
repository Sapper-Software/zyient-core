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

package io.zyient.base.core.connections.settings.common;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.connections.ws.auth.WebServiceAuthHandler;
import io.zyient.base.core.connections.ws.auth.WebServiceAuthSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.concurrent.TimeUnit;


/**
 * <pre>
 *     <connections>
 *         <connection>
 *              <class>[Connection class]</class>
 *              <rest>
 *                  <name>[Connection name, must be unique]</name>
 *                  <endpoint>[Service End Point URL]</endpoint>
 *             </rest>
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
public class WebServiceConnectionSettings extends ConnectionSettings {

    public static class Constants {
        private static final int DEFAULT_READ_TIMEOUT = 5 * 60 * 1000;
        private static final int DEFAULT_CONN_TIMEOUT = 60 * 1000;

        public static final String CONFIG_URL = "endpoint";
        public static final String CONFIG_USE_SSL = "useSSL";
        public static final String CONFIG_READ_TIMEOUT = "timeout.read";
        public static final String CONFIG_CONN_TIMEOUT = "timeout.connection";
        public static final String CONFIG_AUTH_CLASS = "auth.class";
        public static final String CONFIG_AUTH_PATH = "auth";
    }

    @Config(name = Constants.CONFIG_URL)
    private String endpoint;
    @Config(name = Constants.CONFIG_USE_SSL, required = false, type = Boolean.class)
    private boolean useSSL = false;
    @Config(name = Constants.CONFIG_READ_TIMEOUT, required = false, parser = TimeValueParser.class)
    private TimeUnitValue readTimeout = new TimeUnitValue(Constants.DEFAULT_READ_TIMEOUT, TimeUnit.MILLISECONDS);
    @Config(name = Constants.CONFIG_CONN_TIMEOUT, required = false, parser = TimeValueParser.class)
    private TimeUnitValue connectionTimeout = new TimeUnitValue(Constants.DEFAULT_CONN_TIMEOUT, TimeUnit.MILLISECONDS);
    @Config(name = Constants.CONFIG_AUTH_CLASS, required = false, type = Class.class)
    private  Class<? extends WebServiceAuthHandler> authHandler;
    private WebServiceAuthSettings authSettings;

    public WebServiceConnectionSettings() {
        setType(EConnectionType.rest);
    }

    public WebServiceConnectionSettings(@NonNull ConnectionSettings settings) {
        super(settings);
        Preconditions.checkArgument(settings instanceof WebServiceConnectionSettings);
        this.endpoint = ((WebServiceConnectionSettings) settings).getEndpoint();
    }
}
