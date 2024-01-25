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

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class FileInodeLock {
    private String clientId;
    private String fs;
    private String localPath;
    private long timeLocked;
    private long timeUpdated;

    public FileInodeLock() {

    }

    public FileInodeLock(@NonNull String clientId,
                         @NonNull String fs) {
        this.clientId = clientId;
        this.fs = fs;
        this.timeLocked = System.currentTimeMillis();
    }
}
