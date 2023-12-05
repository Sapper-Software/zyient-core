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

package io.zyient.base.core.io.impl.mapped;

import io.zyient.base.core.DistributedLock;
import io.zyient.base.core.io.FileSystem;
import io.zyient.base.core.io.Writer;
import io.zyient.base.core.io.impl.local.LocalWriter;
import io.zyient.base.core.io.model.EFileState;
import io.zyient.base.core.io.model.FileInode;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;

public class MappedWriter extends LocalWriter {
    private MemMappedFile file;

    protected MappedWriter(@NonNull FileInode inode,
                           @NonNull FileSystem fs,
                           boolean overwrite) throws IOException {
        super(inode, fs, overwrite);
    }

    protected MappedWriter(@NonNull FileInode inode,
                           @NonNull FileSystem fs,
                           @NonNull File temp) throws IOException {
        super(inode, fs, temp);
    }

    /**
     * @param overwrite
     * @throws IOException
     */
    @Override
    public Writer open(boolean overwrite) throws IOException, DistributedLock.LockError {
        try (DistributedLock lock = fs.getLock(inode)) {
            lock.lock();
            try {
                inode = (FileInode) fs.fileLock(inode);
                checkLocalCopy(overwrite);
                inode.getLock().setLocalPath(temp.getAbsolutePath());

                file = new MemMappedFile(temp(), inode);
                file.create();

                inode.getState().setState(EFileState.Updating);
                inode = (FileInode) fs.updateInode(inode);

                return this;
            } finally {
                lock.unlock();
            }
        } catch (DistributedLock.LockError le) {
            String err = String.format("[%s][%s] %s", inode.getDomain(), inode.getAbsolutePath(), le.getLocalizedMessage());
            throw new DistributedLock.LockError(err);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void write(byte[] data, int offset, int length) throws IOException {
        checkOpen();
        file.write(data, offset, length);
    }

    @Override
    public void write(int i) throws IOException {
        checkOpen();
        file.write(i);
    }

    /**
     * @throws IOException
     */
    @Override
    public void flush() throws IOException {
        checkOpen();
        file.flush();
    }

    /**
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public long truncate(long offset, long length) throws IOException {
        throw new IOException("Operation not supported...");
    }

    /**
     * @return
     */
    @Override
    public boolean isOpen() {
        return file.isOpen();
    }

    @Override
    public void commit(boolean clearLock) throws IOException {
        checkOpen();
        dataSize = file.writeOffset();
        super.commit(clearLock);
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
        if (file != null) {
            file.flush();
            file.close();
        }
        file = null;
        super.close();
    }
}
