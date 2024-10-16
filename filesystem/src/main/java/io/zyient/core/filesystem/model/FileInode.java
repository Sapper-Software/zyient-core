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

package io.zyient.core.filesystem.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.core.index.Indexed;
import io.zyient.base.core.index.JsonIndexer;
import io.zyient.core.filesystem.encryption.EncryptionType;
import io.zyient.core.filesystem.indexing.InodeIndexConstants;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FileInode extends Inode {
    private FileInodeLock lock;
    @Indexed(name = InodeIndexConstants.NAME_STATE, indexer = JsonIndexer.class)
    private FileState state = new FileState();
    private boolean compressed = false;
    private long syncedSize = 0;
    private long dataSize = 0;
    private EncryptionType encryption = EncryptionType.None;
    private Encrypted encrypted;

    public FileInode() {

    }

    public FileInode(@NonNull String domain,
                     @NonNull String fsPath,
                     @NonNull String name) {
        super(InodeType.File, domain, fsPath, name);
    }
}
