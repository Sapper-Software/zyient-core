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

package io.zyient.base.core.env;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.base.core.state.BaseStateManager;
import io.zyient.base.core.state.BaseStateManagerSettings;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.concurrent.TimeUnit;

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
        private static final String CONFIG_HEARTBEAT = "heartbeat.enable";
        private static final String CONFIG_HEARTBEAT_FREQ = "heartbeat.frequency";
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
    @Config(name = Constants.CONFIG_CONNECTIONS, required = false)
    private String connectionConfigPath;
    @Config(name = Constants.CONFIG_REGISTRY_PATH, required = false)
    private String registryPath;
    @Config(name = Constants.CONFIG_HEARTBEAT, required = false, type = Boolean.class)
    private boolean enableHeartbeat = false;
    @Config(name = Constants.CONFIG_HEARTBEAT_FREQ, required = false, parser = TimeValueParser.class)
    private TimeUnitValue heartbeatFreq = new TimeUnitValue(1L, TimeUnit.MINUTES);
    @Config(name = Constants.CONFIG_STATE_MANAGER_TYPE, required = false, type = Class.class)
    private Class<? extends BaseStateManager> stateManagerClass;
    @Config(name = Constants.CONFIG_BASE_PATH, required = false)
    private String basePath;
    @JsonIgnore
    private HierarchicalConfiguration<ImmutableNode> managersConfig;
}
