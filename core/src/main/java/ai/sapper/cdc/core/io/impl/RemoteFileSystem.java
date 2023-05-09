package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.io.impl.local.LocalFileSystem;
import ai.sapper.cdc.core.io.model.DirectoryInode;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class RemoteFileSystem extends LocalFileSystem {
    private RemoteFsCache cache;

    @Override
    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                     @NonNull BaseEnv<?> env,
                     @NonNull FileSystemConfigReader configReader) throws Exception {
        super.init(config, env, configReader);
        cache = new RemoteFsCache(this);
        cache.init(config);
    }

    public abstract FileInode upload(@NonNull File source, @NonNull DirectoryInode directory) throws IOException;

    public abstract File download(@NonNull FileInode inode) throws IOException;

    public void debug(Object mesg) {
        DefaultLogger.LOGGER.debug(String.format("RESPONSE: %s", mesg));
    }

}
