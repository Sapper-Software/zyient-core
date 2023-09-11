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

package io.zyient.base.core.io.indexing;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.AbstractState;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FileIndexerState extends AbstractState<EFileIndexerState> {
    private long timeCreated;
    private long timeUpdated;
    private String fsZkPath;
    private String fsName;
    private int count;

    public FileIndexerState() {
        super(EFileIndexerState.Error, EFileIndexerState.Unknown);
    }

    public boolean initialized() {
        return (getState() == EFileIndexerState.Initialized
                || getState() == EFileIndexerState.ReIndexing);
    }
}
