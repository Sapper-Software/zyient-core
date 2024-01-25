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

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.DistributedLock;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.Reader;
import io.zyient.core.filesystem.Writer;
import io.zyient.core.filesystem.model.EFileState;
import io.zyient.core.filesystem.model.FileInode;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Getter
@Accessors(fluent = true)
public class LocalWriter extends Writer {
    @Getter(AccessLevel.NONE)
    private FileOutputStream outputStream;
    private final LocalPathInfo path;

    protected LocalWriter(@NonNull FileInode inode,
                          @NonNull FileSystem fs,
                          boolean overwrite) throws IOException {
        super(inode, fs, overwrite);
        if (inode.getPathInfo() == null) {
            path = (LocalPathInfo) fs.parsePathInfo(inode.getURI());
            inode.setPathInfo(path);
        } else {
            path = (LocalPathInfo) inode.getPathInfo();
        }
    }

    protected LocalWriter(@NonNull FileInode inode,
                          @NonNull FileSystem fs,
                          @NonNull File temp) throws IOException {
        super(inode, fs, temp);
        if (inode.getPathInfo() == null) {
            path = (LocalPathInfo) fs.parsePathInfo(inode.getURI());
            inode.setPathInfo(path);
        } else {
            path = (LocalPathInfo) inode.getPathInfo();
        }
    }

    /**
     * @param overwrite
     * @throws IOException
     */
    @Override
    public void doOpen(boolean overwrite) throws IOException, DistributedLock.LockError {
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
            } finally {
                lock.unlock();
            }
        } catch (DistributedLock.LockError le) {
            String err = String.format("[%s][%s] %s", inode.getDomain(), inode.getPath(), le.getLocalizedMessage());
            throw new DistributedLock.LockError(err);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    protected void getLocalCopy() throws Exception {
        if (path.file.exists()) {
            FileUtils.copyFile(path.file, temp);
            temp = Reader.checkDecompress(temp, inode, fs);
        }
    }

    @Override
    public void doWrite(byte @NonNull [] data, int offset, int length) throws IOException {
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

    /**
     * @return
     */
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
            File toUpload = null;
            if (inode.isCompressed()) {
                toUpload = fs.compress(temp);
            } else {
                toUpload = fs.createTmpFile();
                FileUtils.copyFile(temp, toUpload);
            }
            if (toUpload == null) {
                throw new IOException(String.format("Failed to generate upload file. [path=%s]", path.pathConfig()));
            }
            if (path.exists()) {
                if (!path.file.delete()) {
                    throw new IOException(
                            String.format("Failed to delete existing file. [path=%s]", path.file.getAbsolutePath()));
                }
            }
            if (!toUpload.renameTo(path.file)) {
                throw new IOException(
                        String.format("Failed to rename file. [path=%s]", toUpload.getAbsolutePath()));
            }
            try (DistributedLock lock = fs.getLock(inode)) {
                lock.lock();
                try {
                    if (!fs.isFileLocked(inode)) {
                        throw new IOException(
                                String.format("[%s][%s] File not locked or locked by another process.",
                                        inode.getDomain(), inode.getPath()));
                    }
                    String p = inode.getLock().getLocalPath();
                    if (p.compareTo(temp.getAbsolutePath()) != 0) {
                        throw new IOException(String.format("[%s][%s] Local path mismatch. [expected=%s][locked=%s]",
                                inode.getDomain(), inode.getPath(),
                                temp.getAbsolutePath(), p));
                    }

                    inode.setSyncedSize(fileSize(path.file));
                    inode.setDataSize(dataSize);
                    inode.setSyncTimestamp(getLocalUpdateTime());
                    inode.getState().setState(EFileState.Synced);
                    if (clearLock) {
                        inode = (FileInode) fs.fileUnlock(inode);
                        if (!temp.delete()) {
                            throw new IOException(
                                    String.format("Failed to delete local file. [path=%s]", temp.getAbsolutePath()));
                        }
                    } else {
                        inode = (FileInode) fs.fileUpdateLock(inode);
                    }
                    fs.updateInode(inode);
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (outputStream != null) {
            try {
                outputStream.flush();
                outputStream.close();
                outputStream = null;
                if (delete && temp != null && temp.exists()) {
                    if (!temp.delete()) {
                        DefaultLogger.warn(
                                String.format("Failed to delete temporary file. [path=%s]", temp.getAbsolutePath()));
                    }
                }
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
    }
}
