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

package ai.sapper.cdc.core.io;

import ai.sapper.cdc.core.io.model.FileInode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

@Getter
@Accessors(fluent = true)
public abstract class Reader implements Closeable {
    protected final FileInode inode;
    protected final FileSystem fs;

    protected Reader(@NonNull FileInode inode,
                     @NonNull FileSystem fs) {
        this.inode = inode;
        this.fs = fs;
    }

    public abstract Reader open() throws IOException;

    public abstract int read(byte[] buffer, int offset, int length) throws IOException;

    public int read(byte[] buffer) throws IOException {
        return read(buffer, 0, buffer.length);
    }

    public abstract void seek(int offset) throws IOException;

    public abstract boolean isOpen();

    public abstract File copy() throws IOException;

    public static File checkDecompress(@NonNull File infile,
                                       @NonNull FileInode inode,
                                       @NonNull FileSystem fs) throws IOException {
        if (inode.isCompressed()) {
            File outf = fs.decompress(infile);
            if (outf != null) {
                return outf;
            }
        }
        return infile;
    }

    public abstract InputStream getInputStream() throws Exception;
}
