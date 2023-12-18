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

package io.zyient.core.filesystem.impl.mapped;

import io.zyient.core.filesystem.Reader;
import io.zyient.core.filesystem.Writer;
import io.zyient.core.filesystem.impl.local.LocalFileSystem;
import io.zyient.core.filesystem.impl.local.LocalPathInfo;
import io.zyient.core.filesystem.model.FileInode;
import lombok.NonNull;

import java.io.IOException;

public class MappedFileSystem extends LocalFileSystem {
    @Override
    protected Reader getReader(@NonNull FileInode inode) throws IOException {
        LocalPathInfo pi = (LocalPathInfo) inode.getPathInfo();
        if (pi == null) {
            pi = new LocalPathInfo(this, inode);
            inode.setPathInfo(pi);
        }
        if (!pi.exists()) {
            throw new IOException(String.format("Local file not found. [path=%s]", inode.getPath()));
        }
        return new MappedReader(inode, this).open();
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode, boolean overwrite) throws IOException {
        LocalPathInfo pi = (LocalPathInfo) inode.getPathInfo();
        if (pi == null) {
            pi = new LocalPathInfo(this, inode);
            inode.setPathInfo(pi);
        }
        return new MappedWriter(inode, this, overwrite).open(overwrite);
    }
}
