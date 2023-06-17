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

package ai.sapper.cdc.core.connections.settings;

import ai.sapper.cdc.common.config.Config;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;


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
        public static final String CONFIG_URL = "endpoint";
    }

    @Config(name = Constants.CONFIG_URL)
    private String endpoint;

    public WebServiceConnectionSettings() {
        setType(EConnectionType.rest);
    }

    public WebServiceConnectionSettings(@NonNull ConnectionSettings settings) {
        super(settings);
        Preconditions.checkArgument(settings instanceof WebServiceConnectionSettings);
        this.endpoint = ((WebServiceConnectionSettings) settings).getEndpoint();
    }
}
