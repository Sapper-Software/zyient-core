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

package ai.sapper.cdc.core.io.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public abstract class Inode {
    private String fsPath;
    private String uuid;
    private String domain;
    private Map<String, String> path;
    private String absolutePath;
    private long createTimestamp = 0;
    private long updateTimestamp = 0;
    private long syncTimestamp = 0;
    private InodeType type;
    @JsonIgnore
    private Inode parent = null;
    private String parentZkPath;
    private String name;
    private String zkPath;
    @JsonIgnore
    private PathInfo pathInfo;

    public Inode() {

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
        return path.size();
    }

    @Override
    public String toString() {
        return String.format("[ID=%s][PATH=%s]", uuid, path);
    }
}
