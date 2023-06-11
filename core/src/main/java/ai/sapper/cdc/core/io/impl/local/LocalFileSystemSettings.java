package ai.sapper.cdc.core.io.impl.local;

import ai.sapper.cdc.core.io.model.FileSystemSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;


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
 *                         <domain>[Domain name]</domain>
 *                         <path>[Local path (absolute path)]</path>
 *                     </container>
 *                     ...
 *                     <default>[Default container name]</default>
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
public  class LocalFileSystemSettings extends FileSystemSettings {
    public LocalFileSystemSettings() {
        setType(LocalFileSystem.class.getCanonicalName());
    }
}
