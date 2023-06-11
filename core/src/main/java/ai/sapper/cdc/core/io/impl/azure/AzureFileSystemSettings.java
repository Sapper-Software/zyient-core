package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.io.impl.RemoteFileSystemSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;


/**
 * <code>
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
 *                 <client>
 *                     <endpointUrl>[End-point URL]</endpointUrl>
 *                     <authClass>[Authentication implementing class (ai.sapper.cdc.core.io.impl.azure.SharedKeyAuth)]</authClass>
 *                     <account>[Account name]</account>
 *                     <auth>
 *                          <authKey>[Authentication Key]</authKey>
 *                     </auth>
 *                 </client>
 *                 <writer>
 *                     <flush>
 *                         <size>[Flush trigger size (in bytes), default = 32MB]</size>
 *                         <interval>[Flush interval (in milliseconds), default = 1min</interval>
 *                     </flush>
 *                     <threads>[Max #of upload threads, default = 4]</threads>
 *                 </writer>
 *                 <compressed>[true|false, default=false]</compressed>
 *                 <containers>
 *                     <container>
 *                         <domain>[Domain name]</domain>
 *                         <container>[Azure Container name]</container>
 *                         <path>[Domain Root path]</path>
 *                     </container>
 *                     ...
 *                     <default>[Default domain name]</default>
 *                 </containers>
 *             </fileSystem>
 *             ...
 *         </fileSystems>
 *     </fs>
 * </code>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public  class AzureFileSystemSettings extends RemoteFileSystemSettings {
    @Config(name = "uploadTimeout", required = false, type = Long.class)
    private long uploadTimeout = 15; // 15 Seconds
    private AzureFsClientSettings clientSettings;

    public AzureFileSystemSettings() {
        setType(AzureFileSystem.class.getCanonicalName());
    }
}