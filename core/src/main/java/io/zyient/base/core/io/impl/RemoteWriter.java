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

package io.zyient.base.core.io.impl;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.DistributedLock;
import io.zyient.base.core.io.Writer;
import io.zyient.base.core.io.model.EFileState;
import io.zyient.base.core.io.model.FileInode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Getter
@Accessors(fluent = true)
public abstract class RemoteWriter extends Writer {
    private final RemoteFsCache cache;
    private FileOutputStream outputStream;
    private long lastFlushTimestamp;
    private long lastFlushSize;

    protected RemoteWriter(@NonNull FileInode inode,
                           @NonNull RemoteFileSystem fs,
                           boolean overwrite) {
        super(inode, fs, overwrite);
        cache = fs.cache();
    }

    protected RemoteWriter(@NonNull FileInode inode,
                           @NonNull RemoteFileSystem fs,
                           @NonNull File temp) {
        super(inode, fs, temp);
        cache = fs.cache();
    }


    @Override
    public void doOpen(boolean overwrite) throws IOException {
        try (DistributedLock lock = fs.getLock(inode)) {
            lock.lock();
            try {
                inode = (FileInode) fs.fileLock(inode);
                checkLocalCopy(overwrite);
                inode.getLock().setLocalPath(temp.getAbsolutePath());
                outputStream = new FileOutputStream(temp, !overwrite);
                inode.getState().setState(EFileState.Updating);

                inode = (FileInode) fs.updateInode(inode);
                dataSize = inode.getDataSize();
                lastFlushTimestamp = System.currentTimeMillis();
                lastFlushSize = fileSize(temp);
            } finally {
                lock.unlock();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    protected void getLocalCopy() throws Exception {
        if (fs.exists(inode.getPathInfo())) {
            File file = fs.download(inode, cache.settings().getDownloadTimeout());
            if (file == null) return;
            if (inode.isCompressed()) {
                File outf = fs.decompress(file);
                if (!file.delete()) {
                    throw new IOException(String.format("Failed to delete file. [path=%s]", file.getAbsolutePath()));
                }
                if (!outf.renameTo(file)) {
                    throw new IOException(String.format("Filed to rename file. [path=%s]", outf.getAbsolutePath()));
                }
            }
            temp = file;
        }
    }

    @Override
    public void doWrite(byte @NonNull [] data, int offset, int length) throws IOException {
        checkOpen();
        outputStream.write(data, (int) offset, (int) length);
        dataSize += length;
    }

    @Override
    public void write(int i) throws IOException {
        checkOpen();
        outputStream.write(i);
        dataSize += Integer.BYTES;
    }


    /**
     * @throws IOException
     */
    @Override
    public void flush() throws IOException {
        checkOpen();
        outputStream.flush();
        checkUpload();
    }


    private void checkUpload() throws IOException {
        long t = System.currentTimeMillis() - lastFlushTimestamp;
        long s = fileSize(temp) - lastFlushSize;
        RemoteFileSystem rfs = (RemoteFileSystem) fs;
        RemoteFileSystemSettings rs = (RemoteFileSystemSettings) rfs.settings();
        if (t > rs.getWriterFlushInterval() || s > rs.getWriterFlushSize()) {
            commit(false);
        }
    }


    @Override
    public long truncate(int length) throws IOException {
        return truncate(0, length);
    }

    /**
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public long truncate(long offset, long length) throws IOException {
        checkOpen();
        FileChannel channel = outputStream.getChannel();
        channel = channel.truncate(offset + length);
        dataSize = channel.size();

        return dataSize;
    }

    @Override
    public boolean isOpen() {
        return (outputStream != null);
    }

    @Override
    public void doCommit(boolean clearLock) throws IOException {
        try {
            Path tp = Paths.get(temp.toURI());
            if (Files.size(tp) <= 0) {
                return;
            }
            File toUpload = temp;
            if (inode.isCompressed()) {
                toUpload = fs.compress(temp);
            }

            try (DistributedLock lock = fs.getLock(inode)) {
                lock.lock();
                try {
                    if (!fs.isFileLocked(inode)) {
                        throw new IOException(
                                String.format("[%s][%s] File not locked or locked by another process.",
                                        inode.getDomain(), inode.getAbsolutePath()));
                    }
                    String path = inode.getLock().getLocalPath();
                    if (path.compareTo(temp.getAbsolutePath()) != 0) {
                        throw new IOException(String.format("[%s][%s] Local path mismatch. [expected=%s][locked=%s]",
                                inode.getDomain(), inode.getAbsolutePath(),
                                temp.getAbsolutePath(), path));
                    }
                    inode.setSyncedSize(fileSize(temp));
                    inode.setSyncTimestamp(getLocalUpdateTime());
                    inode.getState().setState(EFileState.PendingSync);

                    inode = (FileInode) fs.updateInode(inode);

                    RemoteFileSystem rfs = (RemoteFileSystem) fs;
                    inode = rfs.upload(toUpload, inode, clearLock);
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }


    @Override
    public void close() throws IOException {
        if (isOpen()) {
            flush();
        }
        if (outputStream != null) {
            outputStream.close();
            outputStream = null;
        }
        if (delete && temp != null && temp.exists()) {
            if (!temp.delete()) {
                DefaultLogger.warn(String.format("Failed to delete local copy. [path=%s]", temp.getAbsolutePath()));
            }
        }
    }

    protected abstract String getTmpPath();
}
