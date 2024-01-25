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

package io.zyient.core.mapping.readers;

import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.readers.settings.ReaderSettings;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class InputReader implements Closeable {
    private ReaderSettings settings;
    private File input;
    private InputContentInfo contentInfo;

    public abstract ReadCursor open() throws IOException;

    public abstract List<SourceMap> nextBatch() throws IOException;
}
