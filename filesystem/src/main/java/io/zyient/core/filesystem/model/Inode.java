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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.core.index.Indexed;
import io.zyient.base.core.index.JsonIndexer;
import io.zyient.core.filesystem.indexing.InodeIndexConstants;
import io.zyient.core.sdk.model.fs.FileHandle;
import io.zyient.core.sdk.model.fs.FileId;
import io.zyient.core.sdk.model.fs.FileType;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public abstract class Inode {
    @Indexed(name = InodeIndexConstants.NAME_FS_PATH)
    private String fsPath;
    @Indexed(name = InodeIndexConstants.NAME_UUID)
    private String uuid;
    @Indexed(name = InodeIndexConstants.NAME_DOMAIN)
    private String domain;
    @Indexed(name = InodeIndexConstants.NAME_URI, indexer = JsonIndexer.class)
    private Map<String, String> URI;
    @Indexed(name = InodeIndexConstants.NAME_PATH)
    private String path;
    @Indexed(name = InodeIndexConstants.NAME_CREATE_DATE)
    private long createTimestamp = 0;
    @Indexed(name = InodeIndexConstants.NAME_MODIFIED_DATE)
    private long updateTimestamp = 0;
    private long syncTimestamp = 0;
    @Indexed(name = InodeIndexConstants.NAME_TYPE)
    private InodeType type;
    @JsonIgnore
    private Inode parent = null;
    @Indexed(name = InodeIndexConstants.NAME_PARENT_ZK_PATH)
    private String parentZkPath;
    @Indexed(name = InodeIndexConstants.NAME_NAME)
    private String name;
    @Indexed(name = InodeIndexConstants.NAME_ZK_PATH)
    private String zkPath;
    @JsonIgnore
    private PathInfo pathInfo;
    @Indexed(name = InodeIndexConstants.NAME_ATTRS, indexer = JsonIndexer.class)
    private Map<String, String> attributes;

    public Inode() {
        uuid = UUID.randomUUID().toString();
    }

    public Inode(@NonNull InodeType type,
                 @NonNull String domain,
                 @NonNull String fsPath,
                 @NonNull String name) {
        uuid = UUID.randomUUID().toString();
        this.type = type;
        this.name = name;
        this.domain = domain;
        this.fsPath = fsPath;
    }

    public void setParent(Inode parent) {
        this.parent = parent;
        if (parent != null) {
            parentZkPath = parent.getZkPath();
        } else {
            parentZkPath = null;
        }
    }

    @JsonIgnore
    public boolean isDirectory() {
        return (type == InodeType.Directory);
    }

    @JsonIgnore
    public boolean isFile() {
        return (type == InodeType.File);
    }

    @JsonIgnore
    public boolean isArchive() {
        return (type == InodeType.Archive);
    }

    public long size() throws IOException {
        return URI.size();
    }

    public Inode attribute(@NonNull String key, @NonNull String value) {
        if (attributes == null) {
            attributes = new HashMap<>();
        }
        attributes.put(key, value);
        return this;
    }

    public Inode add(@NonNull Map<String, String> attributes) {
        if (this.attributes == null) {
            this.attributes = attributes;
        } else {
            this.attributes.putAll(attributes);
        }
        return this;
    }

    public String attribute(@NonNull String key) {
        if (attributes != null)
            return attributes.get(key);
        return null;
    }

    public boolean remove(@NonNull String key) {
        if (attributes != null) {
            return (attributes.remove(key) != null);
        }
        return false;
    }

    public FileHandle as() throws IOException {
        FileId id = new FileId();
        id.setId(uuid);
        id.setName(name);
        FileHandle handle = new FileHandle();
        handle.setFileId(id);
        handle.setDomain(domain);
        handle.setPath(path);
        handle.setURI(URI);
        if (isDirectory())
            handle.setType(FileType.Directory);
        else
            handle.setType(FileType.File);
        handle.setCreateTime(createTimestamp);
        handle.setUpdateTime(updateTimestamp);
        handle.setSize(size());
        handle.setAttributes(attributes);
        return handle;
    }

    @Override
    public String toString() {
        return String.format("[ID=%s][DOMAIN=%s][PATH=%s]", uuid, domain, URI);
    }
}
