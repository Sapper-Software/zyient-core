package ai.sapper.cdc.core.io.model;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.model.ESettingsSource;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ArchiverSettings extends Settings {
    public static final String __CONFIG_PATH = "archiver";
    public static final String TEMP_PATH = String.format("%s/zyient/cdc/archive",
            System.getProperty("java.io.tmpdir"));

    public static class Constants {
        public static final String CONFIG_FS_CLASS = "type";
        public static final String CONFIG_NAME = "name";
        public static final String CONFIG_ZK_CONNECTION = "zk.connection";
        public static final String CONFIG_ZK_PATH = "zk.path";
        public static final String CONFIG_ZK_LOCK_TIMEOUT = "zk.lockTimeout";
        public static final String CONFIG_DEFAULT_COMPRESSED = "compressed";
        public static final String CONFIG_TEMP_FOLDER = "tmp.path";
        public static final int LOCK_TIMEOUT = 60 * 1000;
    }

    @Config(name = Constants.CONFIG_FS_CLASS)
    private String type;
    @Config(name = Constants.CONFIG_NAME)
    private String name;
    @Config(name = Constants.CONFIG_ZK_CONNECTION, required = false)
    private String zkConnection;
    @Config(name = Constants.CONFIG_ZK_PATH, required = false)
    private String zkPath;
    @Config(name = Constants.CONFIG_TEMP_FOLDER, required = false)
    private String tempDir = TEMP_PATH;
    @Config(name = Constants.CONFIG_ZK_LOCK_TIMEOUT, required = false, type = Integer.class)
    private int lockTimeout = Constants.LOCK_TIMEOUT;
    @Config(name = Constants.CONFIG_DEFAULT_COMPRESSED, required = false, type = Boolean.class)
    private boolean compressed = false;
    private Container defaultContainer;
    private Map<String, Container> containers;
    private ESettingsSource source = ESettingsSource.File;
}
