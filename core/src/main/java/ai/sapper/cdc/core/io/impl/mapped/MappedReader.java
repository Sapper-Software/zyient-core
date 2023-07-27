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

package ai.sapper.cdc.core.io.impl.mapped;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.impl.local.LocalReader;
import ai.sapper.cdc.core.io.model.FileInode;
import com.google.common.base.Preconditions;
import lombok.NonNull;

import java.io.IOException;

public class MappedReader extends LocalReader {
    private MemMappedFile file;

    public MappedReader(@NonNull FileInode inode,
                        @NonNull FileSystem fs) throws IOException {
        super(inode, fs);
        Preconditions.checkArgument(fs instanceof MappedFileSystem);
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public Reader open() throws IOException {
        synchronized (this) {
            super.open();
            try {
                file = new MemMappedFile(temp(), inode);
                file.forRead();
                return this;
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
    }

    /**
     * @param buffer
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", inode().toString()));
        }
        return file.read(buffer, offset, length);
    }

    /**
     * @param offset
     * @throws IOException
     */
    @Override
    public void seek(int offset) throws IOException {
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", inode().toString()));
        }
        synchronized (this) {
            file.seek(offset);
        }
    }

    /**
     * @return
     */
    @Override
    public boolean isOpen() {
        return file.isOpen();
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
        synchronized (this) {
            super.close();
            if (file != null) {
                file.close();
            }
            file = null;
        }
    }
}
