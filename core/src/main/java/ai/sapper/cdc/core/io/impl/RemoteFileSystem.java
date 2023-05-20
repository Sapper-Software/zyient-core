package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.impl.local.LocalContainer;
import ai.sapper.cdc.core.io.model.Container;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.io.model.Inode;
import ai.sapper.cdc.core.io.model.PathInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Getter
@Accessors(fluent = true)
public abstract class RemoteFileSystem extends FileSystem implements FileUploadCallback {
    private RemoteFsCache cache;
    protected ExecutorService uploader;

    @Override
    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                      @NonNull BaseEnv<?> env,
                      @NonNull FileSystemConfigReader configReader) throws Exception {
        super.init(config, env, configReader);
        Preconditions.checkArgument(configReader instanceof RemoteFileSystemConfigReader);
        cache = new RemoteFsCache(this);
        cache.init(config);
        RemoteFileSystemSettings settings = (RemoteFileSystemSettings) settings();
        uploader =
                new ThreadPoolExecutor(settings.getUploadThreadCount(),
                        settings.getUploadThreadCount(),
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>());
    }

    public abstract FileInode upload(@NonNull File source,
                                     @NonNull FileInode path,
                                     boolean clearLock) throws IOException;

    public abstract File download(@NonNull FileInode inode) throws IOException;

    public abstract long size(@NonNull PathInfo path) throws IOException;

    public void debug(Object mesg) {
        DefaultLogger.debug(String.format("RESPONSE: %s", mesg));
    }

    public static class RemoteFileSystemConfigReader extends FileSystemConfigReader {

        public RemoteFileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                            @NonNull String path,
                                            @NonNull Class<? extends RemoteFileSystemSettings> type,
                                            @NonNull Class<? extends Container> containerType) {
            super(config, path, type, containerType);
        }

        public RemoteFileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                            @NonNull Class<? extends RemoteFileSystemSettings> type,
                                            @NonNull Class<? extends Container> containerType) {
            super(config, type, containerType);
        }
    }


    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public Inode get(@NonNull PathInfo path) throws IOException {
        Inode node = getInode(path);
        if (node != null) {
            node.setPathInfo(parsePathInfo(node.getPath()));
        }
        return node;
    }

    @Override
    protected String getAbsolutePath(@NonNull String path,
                                     @NonNull String domain) throws IOException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(domain));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        Container container = domainMap.get(domain);
        if (!(container instanceof LocalContainer)) {
            throw new IOException(String.format("Mapped container not found. [domain=%s]", domain));
        }
        String pp = ((LocalContainer) container).getPath();

        return PathUtils.formatPath(String.format("/%s/%s/%s",
                pp, domain, path));
    }

    @Override
    public void close() throws IOException {
        super.close();
        uploader.shutdown();
        try {
            if (!uploader.awaitTermination(60, TimeUnit.SECONDS)) {
                uploader.shutdownNow();
            }
        } catch (InterruptedException e) {
            uploader.shutdownNow();
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static abstract class FileUploader implements Runnable {
        protected final RemoteFileSystem fs;
        protected final FileInode inode;
        private final FileUploadCallback callback;
        private final boolean clearLock;

        protected FileUploader(@NonNull RemoteFileSystem fs,
                               @NonNull FileInode inode,
                               @NonNull FileUploadCallback callback,
                               boolean clearLock) {
            this.fs = fs;
            this.inode = inode;
            this.callback = callback;
            this.clearLock = clearLock;
        }

        @Override
        public void run() {
            try {
                Object response = upload();
                callback.onSuccess(inode, response, clearLock);
            } catch (Throwable t) {
                DefaultLogger.stacktrace(t);
                DefaultLogger.error(
                        String.format("Upload failed. [domain=%s][path=%s]", inode.getDomain(), inode.getPath()));
                callback.onError(inode, t);
            }
        }

        protected abstract Object upload() throws Throwable;
    }
}
