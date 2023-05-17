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
public abstract class FileSystemSettings extends Settings {
    public static final String TEMP_PATH = String.format("%s/zyient/cdc",
            System.getProperty("java.io.tmpdir"));

    public static final String CONFIG_FS_CLASS = "type";
    public static final String CONFIG_NAME = "name";
    public static final String CONFIG_ID = "id";
    public static final String CONFIG_TEMP_FOLDER = "tmp.path";
    public static final String CONFIG_TEMP_TTL = "tmp.ttl";
    public static final String CONFIG_TEMP_CLEAN = "tmp.clean";
    public static final String CONFIG_ZK_CONNECTION = "zk.connection";
    public static final String CONFIG_ZK_PATH = "zk.path";
    public static final String CONFIG_ZK_LOCK_TIMEOUT = "zk.lockTimeout";
    public static final int LOCK_TIMEOUT = 60 * 1000;

    @Config(name = CONFIG_FS_CLASS)
    private String type;
    @Config(name = CONFIG_NAME)
    private String name;
    @Config(name = CONFIG_ZK_CONNECTION)
    private String zkConnection;
    @Config(name = CONFIG_ZK_PATH)
    private String zkPath;
    @Config(name = CONFIG_TEMP_FOLDER, required = false)
    private String tempDir = TEMP_PATH;
    @Config(name = CONFIG_TEMP_CLEAN, required = false, type = Boolean.class)
    private boolean cleanTmp = true;
    @Config(name = CONFIG_TEMP_TTL, required = false, type = Long.class)
    private long tempTTL = 15 * 60 * 1000;
    @Config(name = CONFIG_ZK_LOCK_TIMEOUT, required = false, type = Integer.class)
    private int lockTimeout = LOCK_TIMEOUT;
    private Container defaultContainer;
    private Map<String, Container> containers;
    private ESettingsSource source = ESettingsSource.File;
}
