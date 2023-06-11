package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.io.impl.RemoteFileSystemSettings;
import lombok.Getter;
import lombok.Setter;


/**
 * <code>
 *     <fs>
 *         <fileSystems>
 *             <fileSystem>
 *                 <type>[FS class]</type>
 *                 <name>[File System name, must be unique in a namespace]</name>
 *                 <region>[AWS S3 region]</region>
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
 *  *                         <domain>[Domain name]</domain>
 *  *                         <bucket>[S3 Bucket name]</bucket>
 *  *                         <path>[Domain Root path]</path>
 *  *                  </container>
 *                     ...
 *                     <default>[Default domain]</default>
 *                 </containers>
 *             </fileSystem>
 *             ...
 *         </fileSystems>
 *     </fs>
 * </code>
 */
@Getter
@Setter
public  class S3FileSystemSettings extends RemoteFileSystemSettings {
    public static final String CONFIG_REGION = "region";

    @Config(name = CONFIG_REGION)
    private String region;

    public S3FileSystemSettings() {
        setType(S3FileSystem.class.getCanonicalName());
    }
}
