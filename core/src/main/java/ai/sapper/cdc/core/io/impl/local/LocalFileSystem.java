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

package ai.sapper.cdc.core.io.impl.local;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.model.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class LocalFileSystem extends FileSystem {
    private final Class<? extends LocalFileSystemSettings> settingsType;

    public LocalFileSystem() {
        settingsType = LocalFileSystemSettings.class;
    }

    public LocalFileSystem(@NonNull Class<? extends LocalFileSystemSettings> settingsType) {
        this.settingsType = settingsType;
    }

    @Override
    public Class<? extends FileSystemSettings> getSettingsType() {
        return LocalFileSystemSettings.class;
    }

    @Override
    public FileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull BaseEnv<?> env) throws IOException {
        try {
            super.init(config, env, new LocalFileSystemConfigReader(config));
            return postInit();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(LOG, "Error initializing Local FileSystem.", t);
            state().error(t);
            throw new IOException(t);
        }
    }

    @Override
    public FileSystem init(@NonNull FileSystemSettings settings,
                           @NonNull BaseEnv<?> env) throws IOException {
        super.init(settings, env);
        return postInit();
    }

    @Override
    public PathInfo parsePathInfo(@NonNull Map<String, String> values) throws IOException {
        return new LocalPathInfo(this, values);
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

        return PathUtils.formatPath(String.format("/%s/%s",
                pp, path));
    }

    @Override
    public DirectoryInode mkdir(@NonNull DirectoryInode parent,
                                @NonNull String name) throws IOException {
        if (name.indexOf('/') >= 0) {
            throw new IOException(String.format("Invalid directory name: recursive directory creation not supported. [name=%s]", name));
        }
        String path = PathUtils.formatPath(String.format("%s/%s", parent.getAbsolutePath(), name));
        LocalPathInfo pd = (LocalPathInfo) parent.getPathInfo();
        if (pd == null) {
            pd = new LocalPathInfo(this, parent);
            parent.setPathInfo(pd);
        }
        if (!pd.file().exists()) {
            throw new IOException(String.format("Parent directory not found. [path=%s]", pd.file().getAbsolutePath()));
        }
        LocalPathInfo pi = new LocalPathInfo(this, path, pd.domain());
        Inode node = createInode(InodeType.Directory, pi);
        if (!pi.file().exists()) {
            if (!pi.file().mkdir()) {
                throw new IOException(
                        String.format("Error creating directory. [path=%s]", pi.file().getAbsolutePath()));
            }
        }
        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (DirectoryInode) updateInodeWithLock(node);
    }

    @Override
    public DirectoryInode mkdirs(@NonNull String module, @NonNull String path) throws IOException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        path = getAbsolutePath(path, module);
        LocalPathInfo pi = new LocalPathInfo(this, path, module);
        Inode node = createInode(InodeType.Directory, pi);
        if (!pi.file().exists()) {
            if (!pi.file().mkdirs()) {
                throw new IOException(
                        String.format("Error creating directory. [path=%s]", pi.file().getAbsolutePath()));
            }
        }
        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (DirectoryInode) updateInodeWithLock(node);
    }

    @Override
    public FileInode create(@NonNull String domain, @NonNull String path) throws IOException {
        path = getAbsolutePath(path, domain);
        LocalPathInfo pi = new LocalPathInfo(this, path, domain);
        return create(pi);
    }

    @Override
    public FileInode create(@NonNull PathInfo pathInfo) throws IOException {
        Preconditions.checkArgument(pathInfo instanceof LocalPathInfo);
        LocalPathInfo pi = (LocalPathInfo) pathInfo;
        Inode node = createInode(InodeType.File, pi);
        File dir = pi.file().getParentFile();
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException(
                        String.format("Error creating directory. [path=%s]", dir.getAbsolutePath()));
            }
        }
        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        if (node.isFile())
            FileUtils.touch(pi.file);
        return (FileInode) updateInodeWithLock(node);
    }

    @Override
    public FileInode create(@NonNull DirectoryInode dir,
                            @NonNull String name) throws IOException {
        FileInode node = (FileInode) createInode(dir, name, InodeType.File);
        if (node.getPathInfo() == null) {
            PathInfo pi = parsePathInfo(node.getPath());
            node.setPathInfo(pi);
        }
        LocalPathInfo pi = (LocalPathInfo) node.getPathInfo();
        File pdir = pi.file().getParentFile();
        if (!pdir.exists()) {
            if (!pdir.mkdirs()) {
                throw new IOException(
                        String.format("Error creating directory. [path=%s]", dir.getAbsolutePath()));
            }
        }
        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        if (node.isFile())
            FileUtils.touch(pi.file);
        return (FileInode) updateInodeWithLock(node);
    }

    /**
     * @param path
     * @param recursive
     * @return
     * @throws IOException
     */
    @Override
    public boolean delete(@NonNull PathInfo path,
                          boolean recursive) throws IOException {
        Preconditions.checkArgument(path instanceof LocalPathInfo);
        if (deleteInode(path, recursive)) {
            if (path.exists()) {
                if (((LocalPathInfo) path).file().isDirectory() && recursive) {
                    FileUtils.deleteDirectory(((LocalPathInfo) path).file());
                    return !path.exists();
                } else {
                    return ((LocalPathInfo) path).file().delete();
                }
            }
        }
        return false;
    }

    @Override
    protected PathInfo parsePathInfo(@NonNull DirectoryInode parent,
                                     @NonNull String path,
                                     @NonNull InodeType type) {
        String p = PathUtils.formatPath(String.format("%s/%s", parent.getAbsolutePath(), path));
        return new LocalPathInfo(this, p, parent.getDomain());
    }

    @Override
    public boolean exists(@NonNull PathInfo path) throws IOException {
        Inode node = getInode(path);
        if (node != null) {
            LocalPathInfo pi = (LocalPathInfo) parsePathInfo(node.getPath());
            return pi.exists();
        }
        return false;
    }

    @Override
    protected Reader getReader(@NonNull FileInode inode) throws IOException {
        LocalPathInfo pi = (LocalPathInfo) inode.getPathInfo();
        if (pi == null) {
            pi = new LocalPathInfo(this, inode);
            inode.setPathInfo(pi);
        }
        if (!pi.exists()) {
            throw new IOException(String.format("Local file not found. [path=%s]", inode.getAbsolutePath()));
        }
        return new LocalReader(inode, this).open();
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode,
                               boolean overwrite) throws IOException {
        LocalPathInfo pi = (LocalPathInfo) inode.getPathInfo();
        if (pi == null) {
            pi = new LocalPathInfo(this, inode);
            inode.setPathInfo(pi);
        }
        return new LocalWriter(inode, this, overwrite).open();
    }

    @Override
    public FileInode upload(@NonNull File source,
                            @NonNull FileInode inode,
                            boolean clearLock) throws IOException {
        LocalPathInfo pi = null;
        if (inode.getPathInfo() == null) {
            pi = (LocalPathInfo) parsePathInfo(inode.getPath());
            inode.setPathInfo(pi);
        } else {
            pi = (LocalPathInfo) inode.getPathInfo();
        }
        if (pi.exists()) {
            if (!delete(pi, false)) {
                throw new IOException(
                        String.format("Failed to delete existing file. [path=%s]", pi.file.getAbsolutePath()));
            }
        }
        if (!source.renameTo(pi.file)) {
            throw new IOException(
                    String.format("Failed to move file. [source=%s][dest=%s]",
                            source.getAbsolutePath(), pi.file.getAbsolutePath()));
        }
        try {
            inode.setSyncedSize(pi.size());
            inode.setSyncTimestamp(System.currentTimeMillis());
            if (clearLock) {
                inode.getState().setState(EFileState.Synced);
                fileUnlock(inode);
            } else {
                inode.getState().setState(EFileState.Updating);
                fileUpdateLock(inode);
            }
            return (FileInode) updateInodeWithLock(inode);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(
                    String.format("Upload failed. [domain=%s][path=%s]", inode.getDomain(), inode.getPath()));
            throw new IOException(ex);
        }
    }

    @Override
    public File download(@NonNull FileInode inode, long timeout) throws IOException {
        LocalPathInfo pi = null;
        if (inode.getPathInfo() == null) {
            pi = (LocalPathInfo) parsePathInfo(inode.getPath());
            inode.setPathInfo(pi);
        } else {
            pi = (LocalPathInfo) inode.getPathInfo();
        }
        return pi.file();
    }

    public static class LocalFileSystemConfigReader extends FileSystemConfigReader {

        public LocalFileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                           @NonNull String path,
                                           @NonNull Class<? extends LocalFileSystemSettings> type,
                                           @NonNull Class<? extends Container> containerType) {
            super(config, path, type, containerType);
        }

        public LocalFileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, LocalFileSystemSettings.class, LocalContainer.class);
        }
    }
}
