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
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.*;

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

    public RemoteReader open() throws IOException {
        try {
            cacheFile = cache.get(inode());
            if (cacheFile == null || !cacheFile.exists()) {
                throw new IOException(
                        String.format("Error downloading file to local. [path=%s]", inode.getAbsolutePath()));
            }

            inputStream = new RandomAccessFile(cacheFile, "r");
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
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
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", inode().toString()));
        }
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

    @Override
    public InputStream getInputStream() throws Exception {
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", inode().toString()));
        }
        return new FileInputStream(cacheFile);
    }
}
