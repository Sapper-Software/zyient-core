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

package io.zyient.core.mapping.model;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public class MappingResult<T> {
    private final Class<? extends T> type;
    private final File source;
    private List<T> result;

    public MappingResult(@NonNull Class<? extends T> type,
                         @NonNull File source) {
        this.type = type;
        this.source = source;
    }

    public MappingResult<T> add(@NonNull List<T> records) {
        if (result == null) {
            result = records;
        } else {
            result.addAll(records);
        }
        return this;
    }

    public MappingResult<T> add(@NonNull T record) {
        if (result == null) {
            result = new ArrayList<>();
        }
        result.add(record);
        return this;
    }

    public boolean isEmpty() {
        return (result == null || result.isEmpty());
    }

    public int size() {
        if (result != null) {
            return result.size();
        }
        return 0;
    }

    public MappingResult<T> clear() {
        if (result != null) {
            result.clear();
        }
        return this;
    }
}
