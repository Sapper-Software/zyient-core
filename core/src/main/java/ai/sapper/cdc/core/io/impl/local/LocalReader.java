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
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.model.FileInode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.*;

@Getter
@Accessors(fluent = true)
public class LocalReader extends Reader {
    private RandomAccessFile inputStream;
    private final LocalPathInfo path;
    private File temp = null;

    public LocalReader(@NonNull FileInode inode,
                       @NonNull FileSystem fs) throws IOException {
        super(inode, fs);
        if (inode.getPathInfo() == null) {
            path = (LocalPathInfo) fs.parsePathInfo(inode.getPath());
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
    public Reader open() throws IOException {
        if (!path.exists()) {
            throw new IOException(String.format("File not found. [path=%s]", path.file().getAbsolutePath()));
        }
        if (!inode().isCompressed()) {
            inputStream = new RandomAccessFile(path.file(), "r");
        } else {
            temp = fs.decompress(path.file);
            inputStream = new RandomAccessFile(temp, "r");
        }
        return this;
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
        return inputStream.read(buffer, offset, length);
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
        inputStream.seek(offset);
    }

    /**
     * @return
     */
    @Override
    public boolean isOpen() {
        return (inputStream != null);
    }

    @Override
    public File copy() throws IOException {
        return path.file();
    }

    @Override
    public InputStream getInputStream() throws Exception {
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", inode().toString()));
        }
        return new FileInputStream(path.file);
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
