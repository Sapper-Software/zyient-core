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

    @Override
    public byte[] readAllBytes() throws IOException {
        checkOpen();
        if (file.writeOffset() > Integer.MAX_VALUE) {
            throw new IOException(String.format("File size too large. [size=%d]", file.writeOffset()));
        }
        reset();
        byte[] buffer = new byte[(int) file.writeOffset()];
        int s = read(buffer);
        if (s != file.writeOffset()) {
            throw new IOException(
                    String.format("Failed to read all bytes: [expected=%d][read=%d][file=%s]",
                            file.writeOffset(), s, path().file().getAbsolutePath()));
        }
        return buffer;
    }

    @Override
    public byte[] readNBytes(int len) throws IOException {
        checkOpen();
        byte[] buffer = new byte[len];
        int s = read(buffer, 0, len);
        if (s != len) {
            throw new IOException(
                    String.format("Failed to read all bytes: [expected=%d][read=%d][file=%s]",
                            file.writeOffset(), s, path().file().getAbsolutePath()));
        }
        return buffer;
    }

    @Override
    public int readNBytes(byte[] buffer, int off, int len) throws IOException {
        checkOpen();
        return read(buffer, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        checkOpen();
        if (file.readOffset() + n >= file.writeOffset()) {
            n = file.writeOffset() - file.readOffset();
        }
        if (n < 0) {
            throw new IOException("Failed to skip bytes...");
        }
        return file.seek(file.readOffset() + n);
    }

    @Override
    public void skipNBytes(long n) throws IOException {
        skip(n);
    }

    @Override
    public int available() throws IOException {
        checkOpen();
        return (int) (file.writeOffset() - file.readOffset());
    }

    @Override
    public synchronized void mark(int readlimit) {

    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public long seek(long offset) throws IOException {
        checkOpen();
        synchronized (this) {
            return file.seek(offset);
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
    public int read(byte @NonNull [] buffer, int offset, int length) throws IOException {
        checkOpen();
        return file.read(buffer, offset, length);
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
