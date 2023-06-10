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
                = BaseStateManagerSettings.__CONFIG_PATH + ".stateManagerClass";
        private static final String CONFIG_CONNECTIONS = "paths.connections";

    }

    @Config(name = Constants.CONFIG_MODULE)
    private String module;
    @Config(name = Constants.CONFIG_INSTANCE)
    private String instance;
    @Config(name = Constants.CONFIG_CONNECTIONS)
    private String connectionConfigPath;
    @Config(name = Constants.CONFIG_HEARTBEAT, required = false, type = Boolean.class)
    private boolean enableHeartbeat = false;
    @Config(name = Constants.CONFIG_STATE_MANAGER_TYPE, required = false, type = Class.class)
    private Class<? extends BaseStateManager> stateManagerClass;
    @JsonIgnore
    private HierarchicalConfiguration<ImmutableNode> managersConfig;
}
