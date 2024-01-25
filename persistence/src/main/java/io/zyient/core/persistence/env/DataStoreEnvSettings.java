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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.core.env.BaseEnvSettings;
import io.zyient.core.persistence.DataStoreManagerSettings;
import lombok.Getter;
import lombok.Setter;

/**
 * <pre>
 *     <module>[Module name]</module>
 *     <instance>[Instance name, must be unique]</instance>
 *     <heartbeat>
 *         <enable>[true/false, default=false]</enable>
 *         <frequency>[time value, default=60s]</frequency>
 *     </heartbeat>
 *     <state>
 *         <stateManagerClass>[State Manager class]</stateManagerClass>
 *     </state>
 *     <paths>
 *         <connections>[Path for connection definitions]</connections>
 *     </paths>
 *     <managers>
 *         ...
 *     </managers>
 *     <dataStores>[path to Data Store definitions]</dataStores>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DataStoreEnvSettings extends BaseEnvSettings {
    @Config(name = "paths.dataStores", required = false)
    private String dataStoresPath = DataStoreManagerSettings.__CONFIG_PATH;
}
