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

import ai.sapper.cdc.common.AbstractState;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FileState extends AbstractState<EFileState> {
    public FileState() {
        super(EFileState.Error, EFileState.Synced);
    }

    public boolean markedForUpdate() {
        return (getState() == EFileState.Updating
                || getState() == EFileState.PendingSync
                || getState() == EFileState.New);
    }

    public boolean synced() {
        return getState() == EFileState.Synced;
    }

    public boolean available() {
        return getState() == EFileState.Updating || synced();
    }
}
