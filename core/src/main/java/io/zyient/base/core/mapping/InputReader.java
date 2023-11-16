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

package io.zyient.base.core.mapping;

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public abstract class InputReader {
    private final SourceTypes[] supportedTypes;
    private final Class<? extends ReaderSettings> settingsType;
    private ReaderSettings settings;

    public InputReader(SourceTypes[] @NonNull supportedTypes,
                       @NonNull Class<? extends ReaderSettings> settingsType) {
        this.supportedTypes = supportedTypes;
        this.settingsType = settingsType;
    }


    public boolean supports(@NonNull SourceTypes fileType) {
        if (supportedTypes != null) {
            for (SourceTypes t : supportedTypes) {
                if (t == fileType) {
                    return true;
                }
            }
        }
        return false;
    }
}
