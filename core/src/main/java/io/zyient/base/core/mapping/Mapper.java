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

import com.google.common.base.Strings;
import io.zyient.base.core.mapping.mapper.Mapping;
import io.zyient.base.core.mapping.model.MappingResult;
import io.zyient.base.core.utils.FileUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class Mapper<T> {
    private final Class<? extends T> targetType;

    public Mapper(@NonNull Class<? extends T> targetType) {
        this.targetType = targetType;
    }

    public MappingResult<T> read(@NonNull File source,
                                 @NonNull SourceTypes sourceType,
                                 @NonNull Mapping<T> mapping) throws Exception {
        if (!source.exists() || !source.canRead()) {
            throw new IOException(String.format("Invalid source, file not found or not readable. [path=%s]",
                    source.getAbsolutePath()));
        }
        if (sourceType == SourceTypes.UNKNOWN) {
            String mimeType = FileUtils.detectMimeType(source);
            if (mimeType.compareToIgnoreCase(FileUtils.MIME_TYPE_TEXT) == 0) {
                sourceType = detectFromName(source);
                if (sourceType == SourceTypes.UNKNOWN)
                    throw new IOException(String.format("Failed to detect source type. [path=%s]",
                            source.getAbsolutePath()));
            }
        }
        return null;
    }

    public SourceTypes detectFromName(@NonNull File file) throws Exception {
        String fname = FilenameUtils.getName(file.getAbsolutePath());
        if (!Strings.isNullOrEmpty(fname)) {
            String ext = FilenameUtils.getExtension(fname);
            if (!Strings.isNullOrEmpty(ext)) {
                SourceTypes t = SourceTypes.fromExtension(ext);
                if (t != SourceTypes.UNKNOWN) {
                    return t;
                }
            }
        }
        return SourceTypes.UNKNOWN;
    }
}
