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

package io.zyient.base.core.io.impl.sftp;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.io.FileSystem;
import io.zyient.base.core.io.Reader;
import io.zyient.base.core.io.Writer;
import io.zyient.base.core.io.impl.RemoteFileSystem;
import io.zyient.base.core.io.model.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

import java.io.File;
import java.io.IOException;
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
    public PathInfo parsePathInfo(@NonNull Map<String, String> values) throws IOException {
        return new SftpPathInfo(this, values);
    }

    @Override
    public FileInode create(@NonNull DirectoryInode dir, @NonNull String name) throws IOException {
        return null;
    }

    @Override
    public boolean delete(@NonNull PathInfo path, boolean recursive) throws IOException {
        return false;
    }

    @Override
    protected PathInfo parsePathInfo(@NonNull DirectoryInode parent,
                                     @NonNull String path,
                                     @NonNull InodeType type) throws IOException {
        String p = PathUtils.formatPath(String.format("%s/%s", parent.getAbsolutePath(), path));
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
        return null;
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode, boolean overwrite) throws IOException {
        return null;
    }

    @Override
    protected void doCopy(@NonNull FileInode source, @NonNull FileInode target) throws IOException {

    }

    @Override
    protected PathInfo renameFile(@NonNull FileInode source, @NonNull String name) throws IOException {
        return null;
    }

    @Override
    protected void doRename(@NonNull FileInode source, @NonNull FileInode target) throws IOException {

    }

    @Override
    public FileInode upload(@NonNull File source, @NonNull FileInode path, boolean clearLock) throws IOException {
        return null;
    }

    @Override
    public File download(@NonNull FileInode inode, long timeout) throws IOException {
        return null;
    }

    @Override
    public void onSuccess(@NonNull FileInode inode, @NonNull Object response, boolean clearLock) {

    }

    @Override
    public PathInfo createSubPath(@NonNull PathInfo parent, @NonNull String path) {
        return null;
    }

    @Override
    public PathInfo createPath(@NonNull String domain, @NonNull Container container, @NonNull String path, @NonNull InodeType type) {
        return null;
    }

    @Override
    public long size(@NonNull PathInfo path) throws IOException {
        return 0;
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
}
