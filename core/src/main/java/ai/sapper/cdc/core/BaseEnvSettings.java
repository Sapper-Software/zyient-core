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

package ai.sapper.cdc.core;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.state.BaseStateManager;
import ai.sapper.cdc.core.state.BaseStateManagerSettings;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

/**
 * <pre>
 *     <module>[Module name]</module>
 *     <instance>[Instance name, must be unique]</instance>
 *     <enableHeartbeat>[Enable heartbeat]</enableHeartbeat>
 *     <state>
 *         <stateManagerClass>[State Manager class]</stateManagerClass>
 *     </state>
 *     <paths>
 *         <connections>[Path for connection definitions]</connections>
 *     </paths>
 *     <managers>
 *         ...
 *     </managers>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class BaseEnvSettings extends Settings {
    public static class Constants {
        public static final String __CONFIG_PATH_MANAGERS = "managers";
        private static final String CONFIG_MODULE = "module";
        private static final String CONFIG_INSTANCE = "instance";
        private static final String CONFIG_HEARTBEAT = "enableHeartbeat";
        private static final String CONFIG_STATE_MANAGER_TYPE
                = __CONFIG_PATH_MANAGERS + "." + BaseStateManagerSettings.__CONFIG_PATH + ".stateManagerClass";
        private static final String CONFIG_CONNECTIONS = "paths.connections";
        private static final String CONFIG_REGISTRY_PATH = "paths.registry";
        private static final String CONFIG_BASE_PATH = "paths.root";
    }

    @Config(name = Constants.CONFIG_MODULE)
    private String module;
    @Config(name = Constants.CONFIG_INSTANCE)
    private String instance;
    @Config(name = Constants.CONFIG_CONNECTIONS)
    private String connectionConfigPath;
    @Config(name = Constants.CONFIG_REGISTRY_PATH, required = false)
    private String registryPath;
    @Config(name = Constants.CONFIG_HEARTBEAT, required = false, type = Boolean.class)
    private boolean enableHeartbeat = false;
    @Config(name = Constants.CONFIG_STATE_MANAGER_TYPE, required = false, type = Class.class)
    private Class<? extends BaseStateManager> stateManagerClass;
    @Config(name = Constants.CONFIG_BASE_PATH)
    private String basePath;
    @JsonIgnore
    private HierarchicalConfiguration<ImmutableNode> managersConfig;
}
