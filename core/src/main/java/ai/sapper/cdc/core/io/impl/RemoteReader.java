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

package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.utils.Timer;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

@Getter
@Accessors(fluent = true)
public abstract class RemoteReader extends Reader {
    private final RemoteFsCache cache;
    private File cacheFile;
    private RandomAccessFile inputStream;

    protected RemoteReader(@NonNull FileInode inode,
                           @NonNull RemoteFileSystem fs) {
        super(inode, fs);
        cache = fs.cache();
    }

    @Override
    public void doOpen() throws IOException {
        try {
            cacheFile = cache.get(inode());
            if (cacheFile == null || !cacheFile.exists()) {
                throw new IOException(
                        String.format("Error downloading file to local. [path=%s]", inode.getAbsolutePath()));
            }

            inputStream = new RandomAccessFile(cacheFile, "r");
        } catch (Exception ex) {
            throw new IOException(ex);
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
        try (Timer t = new Timer(metrics().timerFileRead())) {
            if (inputStream.getChannel().size() > Integer.MAX_VALUE) {
                throw new IOException(String.format("File size too large. [size=%d]", inputStream.getChannel().size()));
            }
            byte[] buffer = new byte[(int) inputStream.getChannel().size()];
            int s = read(buffer);
            if (s != inputStream.getChannel().size()) {
                throw new IOException(
                        String.format("Failed to read all bytes: [expected=%d][read=%d][file=%s]",
                                inputStream.getChannel().size(), s, cacheFile.getAbsolutePath()));
            }
            return buffer;
        }
    }

    @Override
    public byte[] readNBytes(int len) throws IOException {
        checkOpen();
        try (Timer t = new Timer(metrics().timerFileRead())) {
            byte[] buffer = new byte[len];
            int s = read(buffer, 0, len);
            if (s != len) {
                throw new IOException(
                        String.format("Failed to read all bytes: [expected=%d][read=%d][file=%s]",
                                inputStream.getChannel().size(), s, cacheFile.getAbsolutePath()));
            }
            return buffer;
        }
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
        checkOpen();
        return cacheFile;
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
        cacheFile = null;
        inputStream = null;
    }
}
