/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.core.filesystem.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.model.*;
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
        settings.setCacheSettings(cache.settings());
        uploader =
                new ThreadPoolExecutor(settings.getUploadThreadCount(),
                        settings.getUploadThreadCount(),
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>());
    }

    @Override
    public FileSystem init(@NonNull FileSystemSettings settings,
                           @NonNull BaseEnv<?> env) throws IOException {
        super.init(settings, env);
        RemoteFileSystemSettings rfs = (RemoteFileSystemSettings) settings();
        try {
            cache = new RemoteFsCache(this);
            cache.init(rfs.getCacheSettings());

            uploader =
                    new ThreadPoolExecutor(rfs.getUploadThreadCount(),
                            rfs.getUploadThreadCount(),
                            0L,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>());
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
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
        return (DirectoryInode) updateInodeWithLock(node);
    }

    @Override
    public DirectoryInode mkdirs(@NonNull String domain, @NonNull String path) throws IOException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        path = getAbsolutePath(path, domain);
        Container container = domainMap.get(domain);
        if (container == null) {
            throw new IOException(String.format("Mapped container not found. [domain=%s]", domain));
        }
        path = PathUtils.formatPath(path);
        PathInfo pi = createPath(domain, container, path, InodeType.Directory);
        Inode node = createInode(InodeType.Directory, pi);

        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (DirectoryInode) updateInodeWithLock(node);
    }


    @Override
    public FileInode create(@NonNull String domain, @NonNull String path) throws IOException {
        path = getAbsolutePath(path, domain);
        Container container = domainMap.get(domain);
        path = PathUtils.formatPath(path);
        PathInfo pi = createPath(domain, container, path, InodeType.File);
        return create(pi);
    }

    @Override
    public FileInode create(@NonNull PathInfo pathInfo) throws IOException {
        Inode node = createInode(InodeType.File, pathInfo);
        if (node.getPath() == null)
            node.setPath(pathInfo.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pathInfo);
        return (FileInode) updateInodeWithLock(node);
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
            updateInodeWithLock(inode);
            for (PostOperationVisitor visitor : visitors()) {
                visitor.visit(PostOperationVisitor.Operation.Upload,
                        PostOperationVisitor.OperationState.Error,
                        inode, error);
            }
        } catch (Exception ex) {
            for (PostOperationVisitor visitor : visitors()) {
                visitor.visit(PostOperationVisitor.Operation.Upload,
                        PostOperationVisitor.OperationState.Error,
                        inode, ex);
            }
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

    public abstract PathInfo createPath(@NonNull String domain,
                                        @NonNull Container container,
                                        @NonNull String path,
                                        @NonNull InodeType type);

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
        if (container == null) {
            throw new IOException(String.format("Mapped container not found. [domain=%s]", domain));
        }
        String pp = container.getPath();

        return PathUtils.formatPath(String.format("/%s/%s",
                pp, path));
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
