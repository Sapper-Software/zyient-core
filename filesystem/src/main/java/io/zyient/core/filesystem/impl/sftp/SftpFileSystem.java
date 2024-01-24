/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.core.filesystem.impl.sftp;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.Reader;
import io.zyient.core.filesystem.Writer;
import io.zyient.core.filesystem.impl.FileUploadCallback;
import io.zyient.core.filesystem.impl.PostOperationVisitor;
import io.zyient.core.filesystem.impl.RemoteFileSystem;
import io.zyient.core.filesystem.impl.s3.S3Container;
import io.zyient.core.filesystem.impl.s3.S3PathInfo;
import io.zyient.core.filesystem.model.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;

public class SftpFileSystem extends RemoteFileSystem {
    private final StandardFileSystemManager manager = new StandardFileSystemManager();

    @Override
    public Class<? extends FileSystemSettings> getSettingsType() {
        return SftpFileSystemSettings.class;
    }

    @Override
    public FileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull BaseEnv<?> env) throws IOException {
        try {
            super.init(config, env, new SftpFileSystemConfigReader(config));
            manager.init();
            return postInit();
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            state().error(ex);
            throw new IOException(ex);
        }
    }

    @Override
    public FileSystem init(@NonNull FileSystemSettings settings,
                           @NonNull BaseEnv<?> env) throws IOException {
        Preconditions.checkArgument(settings instanceof SftpFileSystemSettings);
        try {
            super.init(settings, env);
            manager.init();
            return postInit();
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            state().error(ex);
            throw new IOException(ex);
        }
    }

    @Override
    protected boolean createDomainDir(DirectoryInode dir) throws IOException {
        return true;
    }

    @Override
    public PathInfo parsePathInfo(@NonNull Map<String, String> values) throws IOException {
        return new SftpPathInfo(this, values);
    }

    @Override
    public PathInfo parsePathInfo(@NonNull String domain, @NonNull String path) throws IOException {
        return new SftpPathInfo(this, path, domain);
    }


    @Override
    public FileInode create(@NonNull PathInfo pathInfo) throws IOException {
        Preconditions.checkArgument(pathInfo instanceof SftpPathInfo);
        return super.create(pathInfo);
    }

    @Override
    public FileInode create(@NonNull DirectoryInode dir,
                            @NonNull String name) throws IOException {
        FileInode node = (FileInode) createInode(dir, name, InodeType.File);
        if (node.getPathInfo() == null) {
            PathInfo pi = parsePathInfo(node.getURI());
            node.setPathInfo(pi);
        }
        SftpPathInfo pi = (SftpPathInfo) node.getPathInfo();
        Preconditions.checkNotNull(pi);
        if (node.getURI() == null)
            node.setURI(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (FileInode) updateInodeWithLock(node);
    }

    @Override
    public boolean delete(@NonNull PathInfo path,
                          boolean recursive) throws IOException {
        Preconditions.checkArgument(path instanceof SftpPathInfo);
        SftpPathInfo pi = (SftpPathInfo) path;
        try {
            // Create remote object
            FileObject fileRemote = manager.resolveFile(getConnectionString(pi.path()), createDefaultOptions());
            if (fileRemote.exists()) {
                if (!pi.directory() || !recursive) {
                    return fileRemote.delete();
                } else {
                    return deleteRecursive(fileRemote);
                }
            }
            return false;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private boolean deleteRecursive(FileObject target) throws Exception {
        if (!target.isFile()) {
            FileObject[] children = target.getChildren();
            if (children != null) {
                for (FileObject child : children) {
                    if (!deleteRecursive(child)) {
                        DefaultLogger.warn(String.format("Filed to delete file. [path=%s]", child.getURI()));
                    }
                }
            }
        }
        return target.delete();
    }

    @Override
    protected PathInfo parsePathInfo(@NonNull DirectoryInode parent,
                                     @NonNull String path,
                                     @NonNull InodeType type) throws IOException {
        String p = PathUtils.formatPath(String.format("%s/%s", parent.getPath(), path));
        return new SftpPathInfo(this, p, parent.getDomain());
    }

    @Override
    public boolean exists(@NonNull PathInfo path) throws IOException {
        Preconditions.checkArgument(path instanceof SftpPathInfo);
        try {
            SftpPathInfo pi = (SftpPathInfo) path;
            String conn = getConnectionString(pi.path());
            FileObject fo = manager.resolveFile(conn);
            return fo.exists();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    protected Reader getReader(@NonNull FileInode inode) throws IOException {
        SftpPathInfo pi = (SftpPathInfo) inode.getPathInfo();
        if (pi == null) {
            S3Container container = (S3Container) domainMap.get(inode.getDomain());
            pi = new SftpPathInfo(this, inode);
            inode.setPathInfo(pi);
        }
        if (!pi.exists()) {
            throw new IOException(String.format("Local file not found. [path=%s]", inode.getPath()));
        }
        return new SftpReader(inode, this).open();
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode,
                               boolean overwrite) throws IOException {
        SftpPathInfo pi = (SftpPathInfo) inode.getPathInfo();
        if (pi == null) {
            S3Container container = (S3Container) domainMap.get(inode.getDomain());
            pi = new SftpPathInfo(this, inode);
            inode.setPathInfo(pi);
        }
        return new SftpWriter(inode, this, overwrite).open();
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode, @NonNull File temp) throws IOException {
        SftpPathInfo pi = (SftpPathInfo) inode.getPathInfo();
        if (pi == null) {
            S3Container container = (S3Container) domainMap.get(inode.getDomain());
            pi = new SftpPathInfo(this, inode);
            inode.setPathInfo(pi);
        }
        return new SftpWriter(inode, this, temp).open();
    }

    @Override
    protected void doCopy(@NonNull FileInode source,
                          @NonNull FileInode target) throws IOException {
        SftpPathInfo spi = checkAndGetPath(source);
        SftpPathInfo tpi = checkAndGetPath(target);

        try {
            FileObject fileSource = manager.resolveFile(getConnectionString(spi.path()), createDefaultOptions());

            if (fileSource.exists()) {
                FileObject fileTarget = manager.resolveFile(getConnectionString(tpi.path()), createDefaultOptions());
                fileTarget.copyFrom(fileSource, Selectors.SELECT_SELF);
                DefaultLogger.debug(String.format("Moved file. [source=%s][target=%s]", spi, tpi));
            } else {
                throw new IOException(String.format("Source find not found. [path=%s]", spi));
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    protected PathInfo renameFile(@NonNull FileInode source,
                                  @NonNull String name) throws IOException {
        SftpPathInfo pi = checkAndGetPath(source);
        String path = String.format("%s/%s", pi.parent(), name);
        return new SftpPathInfo(this, path, pi.domain(), InodeType.File);
    }

    @Override
    protected void doMove(@NonNull FileInode source,
                          @NonNull FileInode target) throws IOException {
        SftpPathInfo spi = checkAndGetPath(source);
        SftpPathInfo tpi = checkAndGetPath(target);

        try {
            FileObject fileSource = manager.resolveFile(getConnectionString(spi.path()), createDefaultOptions());

            if (fileSource.exists()) {
                FileObject fileTarget = manager.resolveFile(getConnectionString(tpi.path()), createDefaultOptions());
                fileSource.moveTo(fileTarget);
                DefaultLogger.debug(String.format("Moved file. [source=%s][target=%s]", spi, tpi));
            } else {
                throw new IOException(String.format("Source find not found. [path=%s]", spi));
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public FileInode upload(@NonNull File source,
                            @NonNull FileInode inode,
                            boolean clearLock) throws IOException {
        S3PathInfo path = checkAndGetPath(inode);
        SftpFileUploader task = new SftpFileUploader(this, manager, inode, source, this, clearLock);
        uploader.submit(task);
        return inode;
    }

    @Override
    public File download(@NonNull FileInode inode,
                         long timeout) throws IOException {
        SftpPathInfo path = checkAndGetPath(inode);
        if (exists(path)) {
            if (path.temp() == null) {
                File tmp = getInodeTempPath(path);
                if (tmp.exists()) {
                    if (!tmp.delete()) {
                        throw new IOException(
                                String.format("Error deleting temporary file. [path=%s]", tmp.getAbsolutePath()));
                    }
                }
                path.withTemp(tmp);
            }
            if (!checkInodeAvailable(inode, timeout)) {
                throw new IOException(
                        String.format("Download operation timeout: File not available for download. [path=%s]",
                                inode.getURI()));
            }
            try {
                // Create local file object. Change location if necessary for new downloadFilePath
                FileObject fileLocal = manager.resolveFile(path.temp().toURI());

                // Create remote file object
                FileObject fileRemote = manager.resolveFile(getConnectionString(path.fsPath()), createDefaultOptions());

                // Copy local file to sftp server
                fileLocal.copyFrom(fileRemote, Selectors.SELECT_SELF);

                return path.temp();
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
        return null;
    }


    /**
     * Method to setup default SFTP config
     *
     * @return the FileSystemOptions object containing the specified
     * configuration options
     * @throws Exception
     */
    public static FileSystemOptions createDefaultOptions() throws Exception {
        // Create SFTP options
        FileSystemOptions opts = new FileSystemOptions();

        // SSH Key checking
        SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(opts, "no");

        /*
         * Using the following line will cause VFS to choose File System's Root
         * as VFS's root. If I wanted to use User's home as VFS's root then set
         * 2nd method parameter to "true"
         */
        // Root directory set to user home
        SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, false);

        // Timeout is count by Milliseconds
        SftpFileSystemConfigBuilder.getInstance().setSessionTimeout(opts, Duration.ofMillis(10000));

        return opts;
    }

    @Override
    public void onSuccess(@NonNull FileInode inode,
                          @NonNull Object response,
                          boolean clearLock) {
        Preconditions.checkArgument(response instanceof FileObject);
        try {
            FileObject file = (FileObject) response;
            inode.setSyncedSize(file.getContent().getSize());
            inode.setSyncTimestamp(file.getContent().getLastModifiedTime());
            if (clearLock) {
                inode.getState().setState(EFileState.Synced);
                fileUnlock(inode);
            } else {
                inode.getState().setState(EFileState.Updating);
                fileUpdateLock(inode);
            }
            updateInodeWithLock(inode);
            for (PostOperationVisitor visitor : visitors()) {
                visitor.visit(PostOperationVisitor.Operation.Upload,
                        PostOperationVisitor.OperationState.Completed,
                        inode, null);
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

    @Override
    public PathInfo createSubPath(@NonNull PathInfo parent,
                                  @NonNull String path) {
        Preconditions.checkArgument(parent instanceof SftpPathInfo);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        return new SftpPathInfo(this, path, parent.domain(), InodeType.Directory);
    }

    @Override
    public PathInfo createPath(@NonNull String domain,
                               @NonNull Container container,
                               @NonNull String path,
                               @NonNull InodeType type) {
        Preconditions.checkArgument(container instanceof SftpContainer);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        return new SftpPathInfo(this, path, domain, type);
    }

    @Override
    public long size(@NonNull PathInfo path) throws IOException {
        if (path instanceof SftpPathInfo) {
            try {
                SftpPathInfo pi = (SftpPathInfo) path;
                String conn = getConnectionString(pi.path());
                FileObject fo = manager.resolveFile(conn);
                if (fo.exists()) {
                    return fo.getContent().getSize();
                }
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
        throw new IOException(String.format("Invalid Path handle. [type=%s]", path.getClass().getCanonicalName()));
    }

    private String getConnectionString(String path) throws Exception {
        SftpFileSystemSettings settings = (SftpFileSystemSettings) this.settings;
        String password = env().keyStore().read(settings.getPassKey());
        if (Strings.isNullOrEmpty(password)) {
            throw new Exception(String.format("SFTP Password not found. [passKey=%s]", settings.getPassKey()));
        }
        return String.format("sftp://%s:%s@%s/%s", settings.getUsername(), password, settings.getHost(), path);
    }

    @Override
    public void close() throws IOException {
        super.close();
        manager.close();
    }

    @Getter
    @Accessors(fluent = true)
    public static class SftpFileSystemConfigReader extends RemoteFileSystemConfigReader {
        public SftpFileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, SftpFileSystemSettings.class, SftpContainer.class);
        }
    }


    public static class SftpFileUploader extends FileUploader {
        private final StandardFileSystemManager manager;
        private final File source;

        protected SftpFileUploader(@NonNull RemoteFileSystem fs,
                                   @NonNull StandardFileSystemManager manager,
                                   @NonNull FileInode inode,
                                   @NonNull File source,
                                   @NonNull FileUploadCallback callback,
                                   boolean clearLock) {
            super(fs, inode, callback, clearLock);
            this.manager = manager;
            this.source = source;
        }

        @Override
        protected Object upload() throws Throwable {
            SftpFileSystem fs = (SftpFileSystem) fs();
            SftpPathInfo pi = fs.checkAndGetPath(inode);
            if (!source.exists()) {
                throw new IOException(String.format("Source file not found. [path=%s]", source.getAbsolutePath()));
            }
            // Create local file object
            FileObject fileLocal = manager.resolveFile(source.toURI());

            // Create remote file object
            FileObject fileRemote = manager
                    .resolveFile(fs.getConnectionString(pi.fsPath()), createDefaultOptions());
            /*
             * use createDefaultOptions() in place of fsOptions for all default
             * options - Ashok.
             */

            // Copy local file to sftp server
            fileRemote.copyFrom(fileLocal, Selectors.SELECT_SELF);

            return fileRemote;
        }
    }
}
