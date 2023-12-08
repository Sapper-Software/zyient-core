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

package io.zyient.core.filesystem.sync;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.AbstractState;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FileSystemSyncState extends AbstractState<EFileSystemSyncState> {
    private long timeCreated;
    private long timeSynced;
    private long timeUpdated;
    private String fsZkPath;
    private String fsName;

    public FileSystemSyncState() {
        super(EFileSystemSyncState.Error, EFileSystemSyncState.Unknown);
    }

    @JsonIgnore
    public boolean isRunning() {
        return (getState() == EFileSystemSyncState.Running);
    }
}
