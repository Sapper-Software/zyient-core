package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.impl.local.LocalContainer;
import ai.sapper.cdc.core.io.impl.s3.S3Container;
import ai.sapper.cdc.core.io.model.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FilenameUtils;

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


    @Override
    public DirectoryInode mkdir(@NonNull DirectoryInode parent, @NonNull String name) throws IOException {
        String path = PathUtils.formatPath(String.format("%s/%s", parent.getAbsolutePath(), name));
        PathInfo pp = parsePathInfo(parent.getPath());
        PathInfo pi = createSubPath(pp, path);
        Inode node = createInode(InodeType.Directory, pi);
        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (DirectoryInode) updateInode(node, pi);
    }

    @Override
    public DirectoryInode mkdirs(@NonNull String domain, @NonNull String path) throws IOException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        path = getAbsolutePath(path, domain);
        Container container = domainMap.get(domain);
        if (!(container instanceof S3Container)) {
            throw new IOException(String.format("Mapped container not found. [domain=%s]", domain));
        }

        PathInfo pi = createPath(domain, container, path);
        Inode node = createInode(InodeType.Directory, pi);

        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (DirectoryInode) updateInode(node, pi);
    }


    @Override
    public FileInode create(@NonNull String domain, @NonNull String path) throws IOException {
        path = getAbsolutePath(path, domain);
        Container container = domainMap.get(domain);
        PathInfo pi = createPath(domain, container, path);
        return create(pi);
    }

    @Override
    public FileInode create(@NonNull PathInfo pathInfo) throws IOException {
        Inode node = createInode(InodeType.File, pathInfo);
        if (node.getPath() == null)
            node.setPath(pathInfo.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pathInfo);
        return (FileInode) updateInode(node, pathInfo);
    }


    @Override
    public void onError(@NonNull FileInode inode, @NonNull Throwable error) {
        try {
            String temp = null;
            if (inode.getLock() != null) {
                temp = inode.getLock().getLocalPath();
            }
            DefaultLogger.error(LOG,
                    String.format("Error uploading file. [domain=%s][path=%s][temp=%s][error=%s]",
                            inode.getDomain(),
                            inode.getAbsolutePath(),
                            temp,
                            error.getLocalizedMessage()));
            DefaultLogger.stacktrace(error);
            inode.getState().error(error);
            updateInode(inode, inode.getPathInfo());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(LOG, ex.getLocalizedMessage());
            throw new RuntimeException(ex);
        }
    }


    protected File getInodeTempPath(PathInfo path) throws IOException {
        String dir = FilenameUtils.getPath(path.path());
        dir = PathUtils.formatPath(String.format("%s/%s", path.domain(), dir));
        String fname = FilenameUtils.getName(path.path());
        return createTmpFile(dir, fname);
    }

    public abstract PathInfo createSubPath(@NonNull PathInfo parent, @NonNull String path);

    public abstract PathInfo createPath(@NonNull String domain, @NonNull Container container, @NonNull String path);

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
