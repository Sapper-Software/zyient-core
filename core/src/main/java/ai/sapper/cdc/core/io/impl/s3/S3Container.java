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

package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.ArchivePathInfo;
import ai.sapper.cdc.core.io.model.Container;
import ai.sapper.cdc.core.io.model.InodeType;
import ai.sapper.cdc.core.io.model.PathInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class S3Container extends Container {
    @Config(name = "bucket")
    private String bucket;

    @Override
    public PathInfo pathInfo(@NonNull FileSystem fs) {
        return new S3PathInfo(fs, getDomain(), getBucket(), getPath(), InodeType.Directory);
    }

    @Override
    public ArchivePathInfo pathInfo(@NonNull Archiver archiver) {
        return null;
    }
}
