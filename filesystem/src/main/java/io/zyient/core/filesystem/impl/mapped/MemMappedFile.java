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

package io.zyient.core.filesystem.impl.mapped;

import com.google.common.base.Preconditions;
import io.zyient.core.filesystem.model.FileInode;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.SyncMode;
import net.openhft.chronicle.core.OS;

import java.io.*;

@Getter
@Accessors(fluent = true)
public class MemMappedFile implements Closeable {
    private final File path;
    private final FileInode node;
    private long readOffset;
    private long writeOffset;
    @Getter(AccessLevel.NONE)
    private MappedBytes data;

    public MemMappedFile(@NonNull File path,
                         @NonNull FileInode node) {
        this.path = path;
        this.node = node;
    }

    public MemMappedFile(@NonNull String path,
                         @NonNull FileInode node) {
        this.path = new File(path);
        this.node = node;
    }

    private void init(boolean readOnly) throws IOException {
        if (readOnly) {
            data = MappedBytes.mappedBytes(path, OS.pageSize(), OS.pageSize() / 8, true);
            data.mappedFile().syncMode(SyncMode.NONE);
        } else {
            data = MappedBytes.mappedBytes(path, OS.pageSize(), OS.pageSize() / 8);
            data.mappedFile().syncMode(SyncMode.ASYNC);
        }

        readOffset = 0;
        writeOffset = node.getDataSize();
        data.readLimit(writeOffset);
    }

    protected void forRead() throws IOException {
        init(true);
    }

    protected void create() throws IOException {
        if (path.exists() && path.length() > 0) {
            init(false);
        } else {
            try (OutputStreamWriter outWrite = new OutputStreamWriter(new FileOutputStream(path))) {
                outWrite.append("\n");
            }
            data = MappedBytes.mappedBytes(path, OS.pageSize(), OS.pageSize() / 8);
            data.mappedFile().syncMode(SyncMode.ASYNC);
            writeOffset = readOffset = 0;
        }
    }

    protected long seek(long offset) {
        Preconditions.checkState(isOpen());
        Preconditions.checkArgument(offset >= 0 && offset < data.readLimit());
        if (offset >= data.readLimit()) {
            offset = data.readLimit();
        }
        readOffset = offset;
        data.readPosition(readOffset);
        return offset;
    }

    public void flush() throws IOException {
        data.sync();
    }

    public long write(byte[] buffer, long offset, long length) throws IOException {
        Preconditions.checkState(isOpen());
        data.write(writeOffset, buffer, (int) offset, (int) length);
        writeOffset += length;

        return length;
    }

    public void write(int i) throws IOException {
        Preconditions.checkState(isOpen());
        data.writeInt(writeOffset, i);
        writeOffset += Integer.BYTES;
    }

    public int read(byte[] buffer, int offset, int length) throws IOException {
        Preconditions.checkState(isOpen());
        if (readOffset + length > data.readLimit()) {
            length = (int) (data.readLimit() - readOffset);
        }
        if (length > 0) {
            length = data.read(buffer, offset, length);
            readOffset += length;
            data.readPosition(readOffset);
        }
        return length;
    }

    @Override
    public void close() throws IOException {
        if (data != null) {
            flush();
            data.releaseLast();
            data.close();
        }
        data = null;
    }

    public boolean isOpen() {
        return (data != null && !(data.isClosed() || data.isClosing()));
    }
}
