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

package io.zyient.core.filesystem.impl.local;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.Reader;
import io.zyient.core.filesystem.Writer;
import io.zyient.core.filesystem.model.*;
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
    protected boolean createDomainDir(DirectoryInode dir) throws IOException {
        File d = new File(dir.getFsPath());
        if (!d.exists()) {
            return d.mkdirs();
        }
        return true;
    }

    @Override
    public PathInfo parsePathInfo(@NonNull Map<String, String> values) throws IOException {
        return new LocalPathInfo(this, values);
    }

    @Override
    public PathInfo parsePathInfo(@NonNull String domain, @NonNull String path) throws IOException {
        return new LocalPathInfo(this, path, domain);
    }

    @Override
    public DirectoryInode __mkdir(@NonNull DirectoryInode parent,
                                  @NonNull String name) throws IOException {
        if (name.indexOf('/') >= 0) {
            throw new IOException(String.format("Invalid directory name: recursive directory creation not supported. [name=%s]", name));
        }
        String path = PathUtils.formatPath(String.format("%s/%s", parent.getPath(), name));
        LocalPathInfo pd = checkAndGetPath(parent);
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
        if (node.getURI() == null)
            node.setURI(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (DirectoryInode) updateInodeWithLock(node);
    }

    @Override
    public DirectoryInode __mkdirs(@NonNull String module, @NonNull String path) throws IOException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        LocalPathInfo pi = new LocalPathInfo(this, path, module);
        Inode node = createInode(InodeType.Directory, pi);
        pi = (LocalPathInfo) node.getPathInfo();
        if (!pi.file().exists()) {
            if (!pi.file().mkdirs()) {
                throw new IOException(
                        String.format("Error creating directory. [path=%s]", pi.file().getAbsolutePath()));
            }
        }
        if (node.getURI() == null)
            node.setURI(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (DirectoryInode) updateInodeWithLock(node);
    }

    @Override
    public FileInode __create(@NonNull String domain, @NonNull String path) throws IOException {
        LocalPathInfo pi = new LocalPathInfo(this, path, domain);
        return create(pi);
    }

    @Override
    public FileInode create(@NonNull PathInfo pathInfo) throws IOException {
        Preconditions.checkArgument(pathInfo instanceof LocalPathInfo);
        LocalPathInfo pi = (LocalPathInfo) pathInfo;
        Inode node = createInode(InodeType.File, pi);
        pi = (LocalPathInfo) node.getPathInfo();
        File dir = pi.file().getParentFile();
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException(
                        String.format("Error creating directory. [path=%s]", dir.getAbsolutePath()));
            }
        }
        if (node.getURI() == null)
            node.setURI(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        if (node.isFile() && !pathInfo.exists())
            FileUtils.touch(pi.file);
        return (FileInode) updateInodeWithLock(node);
    }

    @Override
    public FileInode __create(@NonNull DirectoryInode dir,
                              @NonNull String name) throws IOException {
        FileInode node = (FileInode) createInode(dir, name, InodeType.File);
        LocalPathInfo pi = checkAndGetPath(node);
        File pdir = pi.file().getParentFile();
        if (!pdir.exists()) {
            if (!pdir.mkdirs()) {
                throw new IOException(
                        String.format("Error creating directory. [path=%s]", dir.getPath()));
            }
        }
        if (node.getURI() == null)
            node.setURI(pi.pathConfig());
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
            if (((LocalPathInfo) path).file().isDirectory() && recursive) {
                FileUtils.deleteDirectory(((LocalPathInfo) path).file());
                return !path.exists();
            } else {
                return ((LocalPathInfo) path).file().delete();
            }
        }
        return false;
    }

    @Override
    protected PathInfo __parsePathInfo(@NonNull DirectoryInode parent,
                                       @NonNull String path,
                                       @NonNull InodeType type) {
        String p = PathUtils.formatPath(String.format("%s/%s", parent.getPath(), path));
        return new LocalPathInfo(this, p, parent.getDomain());
    }

    @Override
    public boolean exists(@NonNull PathInfo path) throws IOException {
        Inode node = getInode(path);
        if (node != null) {
            LocalPathInfo pi = (LocalPathInfo) parsePathInfo(node.getURI());
            return pi.file.exists();
        }
        return false;
    }

    @Override
    protected Reader getReader(@NonNull FileInode inode) throws IOException {
        LocalPathInfo pi = checkAndGetPath(inode);
        if (!pi.exists()) {
            throw new IOException(String.format("Local file not found. [path=%s]", inode.getPath()));
        }
        return new LocalReader(inode, this).open();
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode,
                               boolean overwrite) throws IOException {
        LocalPathInfo pi = checkAndGetPath(inode);
        return new LocalWriter(inode, this, overwrite).open();
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode, @NonNull File temp) throws IOException {
        LocalPathInfo pi = checkAndGetPath(inode);
        return new LocalWriter(inode, this, temp).open();
    }

    @Override
    protected void doCopy(@NonNull FileInode source, @NonNull FileInode target) throws IOException {
        LocalPathInfo sp = checkAndGetPath(source);
        LocalPathInfo tp = checkAndGetPath(target);
        if (tp.file.exists()) {
            if (!tp.file.delete()) {
                throw new IOException(
                        String.format("Failed to delete existing file. [path=%s]", tp.file.getAbsolutePath()));
            }
        }
        FileUtils.copyFile(sp.file, tp.file);
    }

    @Override
    protected PathInfo renameFile(@NonNull FileInode source, @NonNull String name) throws IOException {
        LocalPathInfo pi = checkAndGetPath(source);
        String dir = pi.file.getParent();
        String path = String.format("%s/%s", dir, name);
        return new LocalPathInfo(this, path, source.getDomain());
    }

    @Override
    protected void doMove(@NonNull FileInode source,
                          @NonNull FileInode target) throws IOException {
        LocalPathInfo sp = checkAndGetPath(source);
        LocalPathInfo tp = checkAndGetPath(target);
        if (tp.file.exists()) {
            if (!tp.file.delete()) {
                throw new IOException(
                        String.format("Failed to delete existing file. [path=%s]", tp.file.getAbsolutePath()));
            }
        }
        if (!sp.file.renameTo(tp.file)) {
            throw new IOException(
                    String.format("Failed to rename file. [source=%s][target=%s]",
                            sp.file.getAbsolutePath(), tp.file.getAbsolutePath()));
        }
    }

    @Override
    public FileInode upload(@NonNull File source,
                            @NonNull FileInode inode,
                            boolean clearLock) throws IOException {
        LocalPathInfo pi = checkAndGetPath(inode);
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
                    String.format("Upload failed. [domain=%s][path=%s]", inode.getDomain(), inode.getURI()));
            throw new IOException(ex);
        }
    }

    @Override
    public File download(@NonNull FileInode inode, long timeout) throws IOException {
        LocalPathInfo pi = checkAndGetPath(inode);
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
