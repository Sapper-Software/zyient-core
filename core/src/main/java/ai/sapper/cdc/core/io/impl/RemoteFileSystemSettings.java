package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.io.model.FileSystemSettings;
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
 *                         ...
 *                     </container>
 *                     ...
 *                     <default>[Default container name]</default>
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
public class RemoteFileSystemSettings extends FileSystemSettings {
    public static final String CONFIG_WRITER_FLUSH_INTERVAL = "writer.flush.interval";
    public static final String CONFIG_WRITER_FLUSH_SIZE = "writer.flush.size";
    public static final String CONFIG_WRITER_UPLOAD_THREADS = "writer.threads";
    private static final long DEFAULT_WRITER_FLUSH_INTERVAL = 60 * 1000; // 1min
    private static final long DEFAULT_WRITER_FLUSH_SIZE = 1024 * 1024 * 32; //32MB
    private static final int DEFAULT_UPLOAD_THREAD_COUNT = 4;

    @Config(name = CONFIG_WRITER_FLUSH_INTERVAL, required = false, type = Long.class)
    private long writerFlushInterval = DEFAULT_WRITER_FLUSH_INTERVAL;
    @Config(name = CONFIG_WRITER_FLUSH_SIZE, required = false, type = Long.class)
    private long writerFlushSize = DEFAULT_WRITER_FLUSH_SIZE;
    @Config(name = CONFIG_WRITER_UPLOAD_THREADS, required = false, type = Integer.class)
    private int uploadThreadCount = DEFAULT_UPLOAD_THREAD_COUNT;
}
