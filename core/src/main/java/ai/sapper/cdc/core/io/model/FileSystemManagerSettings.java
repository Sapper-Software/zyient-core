package ai.sapper.cdc.core.io.model;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

/**
 * <pre>
 *     <fs>
 *         <zkPath>[zookeeper path]</zkPath>
 *         <zkConnection>[zookeeper connection name]</zkConnection>
 *         <autoSave>[true|false, default=true]</autoSave>
 *     </fs>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FileSystemManagerSettings extends Settings {
    @Config(name = "zkPath")
    private String zkBasePath;
    @Config(name = "zkConnection")
    private String zkConnection;
    @Config(name = "autoSave", required = false, type = Boolean.class)
    private boolean autoSave = true;
}