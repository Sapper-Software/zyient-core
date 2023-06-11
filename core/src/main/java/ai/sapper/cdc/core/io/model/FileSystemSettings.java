package ai.sapper.cdc.core.io.model;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.model.ESettingsSource;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * <pre>
 *     <fs>
 *         <fileSystems>
 *             <fileSystem>
 *                 <type>[FS class]</type>
 *                 <name>[File System name, must be unique in a namespace]</name>
 *                 <tmp>
 *                     <path>[temporary storage path (optional)</path>
 *                     <clean>[true|false, default=true]</clean>
 *                     <ttl>[Clean files after, in milliseconds]</ttl>
 *                 </tmp>
 *                 <zk> -- Optional
 *                     <connection>[zookeeper connection name]</connection>
 *                     <path>[zookeeper path]</path>
 *                     <lockTimeout>[distributed lock timeout (optional)</lockTimeout>
 *                 </zk>
 *                 <compressed>[true|false, default=false]</compressed>
 *                 <containers>
 *                     <container>
 *                         ...
 *                     </container>
 *                     ...
 *                     <default>[Default domain]</default>
 *                 </containers>
 *             </fileSystem>
 *             ...
 *         </fileSystems>
 *     </fs>
 * </pre>
 */
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
    public static final String CONFIG_DEFAULT_COMPRESSED = "compressed";
    public static final int LOCK_TIMEOUT = 60 * 1000;

    @Config(name = CONFIG_FS_CLASS)
    private String type;
    @Config(name = CONFIG_NAME)
    private String name;
    @Config(name = CONFIG_ZK_CONNECTION, required = false)
    private String zkConnection;
    @Config(name = CONFIG_ZK_PATH, required = false)
    private String zkPath;
    @Config(name = CONFIG_TEMP_FOLDER, required = false)
    private String tempDir = TEMP_PATH;
    @Config(name = CONFIG_TEMP_CLEAN, required = false, type = Boolean.class)
    private boolean cleanTmp = true;
    @Config(name = CONFIG_TEMP_TTL, required = false, type = Long.class)
    private long tempTTL = 15 * 60 * 1000;
    @Config(name = CONFIG_ZK_LOCK_TIMEOUT, required = false, type = Integer.class)
    private int lockTimeout = LOCK_TIMEOUT;
    @Config(name = CONFIG_DEFAULT_COMPRESSED, required = false, type = Boolean.class)
    private boolean compressed = false;
    private Container defaultContainer;
    private Map<String, Container> containers;
    private ESettingsSource source = ESettingsSource.File;
}
