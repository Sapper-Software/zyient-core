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

package io.zyient.base.core.mapping.writers;

import io.zyient.base.core.mapping.model.OutputContentInfo;
import io.zyient.base.core.mapping.writers.settings.OutputWriterSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class OutputWriter implements Closeable {
    private File output;
    private OutputWriterSettings settings;
    private OutputContentInfo contentInfo;

    public abstract OutputWriter open() throws IOException;

    public abstract int write(@NonNull List<Map<String, Object>> batch) throws IOException;

    public abstract void flush() throws IOException;
}