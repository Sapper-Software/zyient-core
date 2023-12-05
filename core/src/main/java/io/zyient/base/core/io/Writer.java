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

package io.zyient.base.core.io;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.DistributedLock;
import io.zyient.base.core.io.model.FileInode;
import io.zyient.base.core.io.model.FileSystemMetrics;
import io.zyient.base.core.utils.Timer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

@Getter
@Accessors(fluent = true)
public abstract class Writer extends OutputStream implements Closeable {
    protected FileInode inode;
    protected final FileSystem fs;
    private final boolean overwrite;
    private final FileSystemMetrics metrics;
    @Getter(AccessLevel.PROTECTED)
    protected File temp;
    protected long dataSize;
    protected final boolean delete;

    protected Writer(@NonNull FileInode inode,
                     @NonNull FileSystem fs,
                     boolean overwrite) {
        Preconditions.checkArgument(inode.getPathInfo() != null);
        this.inode = inode;
        this.fs = fs;
        this.metrics = fs.metrics();
        this.overwrite = overwrite;
        delete = true;
    }

    protected Writer(@NonNull FileInode inode,
                     @NonNull FileSystem fs,
                     @NonNull File temp) {
        Preconditions.checkArgument(inode.getPathInfo() != null);
        this.inode = inode;
        this.fs = fs;
        this.metrics = fs.metrics();
        this.overwrite = false;
        this.temp = temp;
        delete = false;
    }

    public Writer open(boolean overwrite) throws IOException, DistributedLock.LockError {
        try (Timer t = new Timer(metrics.timerFileWriteOpen())) {
            doOpen(overwrite);
        }
        return this;
    }

    protected abstract void doOpen(boolean overwrite) throws IOException, DistributedLock.LockError;

    public Writer open() throws IOException {
        return open(overwrite);
    }

    public long fileSize(@NonNull File temp) throws IOException {
        if (temp.exists()) {
            Path p = Paths.get(temp.toURI());
            return Files.size(p);
        }
        return 0;
    }

    @Override
    public void write(byte @NonNull [] data) throws IOException {
        write(data, 0, data.length);
    }

    public void write(byte @NonNull [] data, int length) throws IOException {
        write(data, 0, length);
    }

    @Override
    public void write(byte @NonNull [] b, int off, int len) throws IOException {
        checkOpen();
        try (Timer t = new Timer(metrics.timerFileWrite())) {
            doWrite(b, off, len);
        }
    }

    public abstract void doWrite(byte @NonNull [] b, int off, int len) throws IOException;

    public abstract long truncate(long offset, long length) throws IOException;

    public long truncate(int length) throws IOException {
        return truncate(0, length);
    }

    public abstract boolean isOpen();

    public void commit(boolean clearLock) throws IOException {
        try (Timer t = new Timer(metrics.timerFileWriteCommit())) {
            doCommit(clearLock);
        }
    }

    public abstract void doCommit(boolean clearLock) throws IOException;


    protected void checkLocalCopy(boolean overwrite) throws Exception {
        if (!Strings.isNullOrEmpty(inode.getLock().getLocalPath())) {
            temp = new File(inode.getLock().getLocalPath());
        } else {
            temp = fs.createTmpFile(null, inode.getName());
        }
        if (overwrite) {
            if (temp.exists()) {
                if (!temp.delete()) {
                    throw new IOException(
                            String.format("Failed to delete temp file. [path=%s]", temp.getAbsolutePath()));
                }
            }
        } else {
            if (temp.exists()) {
                if (fs.exists(inode.getPathInfo())) {
                    long uts = getLocalUpdateTime();
                    if (uts < inode.getSyncTimestamp()) {
                        DefaultLogger.info(String.format("Local copy is stale. [inode=%s]", inode.toString()));
                        if (!temp.delete()) {
                            DefaultLogger.warn(
                                    String.format("Failed to delete stale copy. [path=%s]", temp.getAbsolutePath()));
                        }
                        getLocalCopy();
                    }
                }
            } else {
                getLocalCopy();
            }
        }
    }

    protected long getLocalUpdateTime() throws IOException {
        return getLocalUpdateTime(temp);
    }

    private long getLocalUpdateTime(File file) throws IOException {
        if (file.exists()) {
            Path p = Paths.get(file.toURI());
            BasicFileAttributes attr =
                    Files.readAttributes(p, BasicFileAttributes.class);
            FileTime ft = attr.lastModifiedTime();
            return ft.toMillis();
        }
        return 0;
    }

    protected void checkOpen() throws IOException {
        if (!isOpen())
            throw new IOException(String.format("Writer not open: [path=%s]", inode().toString()));
    }

    protected abstract void getLocalCopy() throws Exception;
}
