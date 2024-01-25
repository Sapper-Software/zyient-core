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
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.Reader;
import io.zyient.core.filesystem.model.FileInode;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

@Getter
@Accessors(fluent = true)
public class LocalReader extends Reader {
    @Getter(AccessLevel.NONE)
    private RandomAccessFile inputStream;
    private final LocalPathInfo path;
    private File temp = null;

    public LocalReader(@NonNull FileInode inode,
                       @NonNull FileSystem fs) throws IOException {
        super(inode, fs);
        if (inode.getPathInfo() == null) {
            path = (LocalPathInfo) fs.parsePathInfo(inode.getURI());
            inode.setPathInfo(path);
        } else {
            path = (LocalPathInfo) inode.getPathInfo();
        }
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public void doOpen() throws IOException {
        if (!path.exists()) {
            throw new IOException(String.format("File not found. [path=%s]", path.file().getAbsolutePath()));
        }
        if (!inode().isCompressed()) {
            inputStream = new RandomAccessFile(path.file(), "r");
        } else {
            temp = fs.decompress(path.file);
            inputStream = new RandomAccessFile(temp, "r");
        }
    }

    @Override
    public int read() throws IOException {
        return inputStream.read();
    }

    /**
     * @param buffer
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public int doRead(byte @NonNull [] buffer, int offset, int length) throws IOException {
        return inputStream.read(buffer, offset, length);
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        checkOpen();
        if (inputStream.getChannel().size() > Integer.MAX_VALUE) {
            throw new IOException(String.format("File size too large. [size=%d]", inputStream.getChannel().size()));
        }
        reset();
        byte[] buffer = new byte[(int) inputStream.getChannel().size()];
        int s = read(buffer);
        if (s != inputStream.getChannel().size()) {
            throw new IOException(
                    String.format("Failed to read all bytes: [expected=%d][read=%d][file=%s]",
                            inputStream.getChannel().size(), s, path().file.getAbsolutePath()));
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
                            inputStream.getChannel().size(), s, path().file.getAbsolutePath()));
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
        return inputStream.skipBytes((int) n);
    }

    @Override
    public void skipNBytes(long n) throws IOException {
        skip(n);
    }

    @Override
    public int available() throws IOException {
        checkOpen();
        long pos = inputStream.getChannel().position();
        long size = inputStream.getChannel().size();
        return (int) (size - pos);
    }

    @Override
    public synchronized void mark(int readlimit) {

    }

    @Override
    public synchronized void reset() throws IOException {
        seek(0);
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public long transferTo(OutputStream out) throws IOException {
        throw new IOException("Not supported...");
    }

    /**
     * @return
     */
    @Override
    public boolean isOpen() {
        return (inputStream != null);
    }

    @Override
    public long seek(long offset) throws IOException {
        checkOpen();
        if (offset >= inputStream.getChannel().size()) {
            offset = inputStream.getChannel().size();
        }
        inputStream.seek(offset);
        return offset;
    }

    @Override
    public File copy() throws IOException {
        return path.file();
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
        if (inputStream != null) {
            inputStream.close();
            inputStream = null;
        }
        if (temp != null) {
            if (!temp.delete()) {
                DefaultLogger.error(
                        String.format("Failed to delete temporary file. [path=%s]", temp.getAbsolutePath()));
            }
            temp = null;
        }
    }
}
