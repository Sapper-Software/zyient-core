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

package io.zyient.core.filesystem.impl.local;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.core.filesystem.Archiver;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.model.ArchivePathInfo;
import io.zyient.core.filesystem.model.Container;
import io.zyient.core.filesystem.model.PathInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class LocalContainer extends Container {

    @Override
    public PathInfo pathInfo(@NonNull FileSystem fs) {
        return new LocalPathInfo(fs, "/", getDomain());
    }

    @Override
    public ArchivePathInfo pathInfo(@NonNull Archiver archiver) {
        throw new RuntimeException("Method should not be called...");
    }
}
