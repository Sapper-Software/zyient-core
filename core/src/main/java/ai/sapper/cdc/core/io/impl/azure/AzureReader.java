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

package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.core.io.impl.RemoteReader;
import ai.sapper.cdc.core.io.model.FileInode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class AzureReader extends RemoteReader {
    private final AzurePathInfo pathInfo;

    public AzureReader(@NonNull AzureFileSystem fs,
                       @NonNull FileInode path) throws IOException {
        super(path, fs);
        if (path.getPathInfo() != null) {
            pathInfo = (AzurePathInfo) path.getPathInfo();
        } else {
            pathInfo = (AzurePathInfo) fs.parsePathInfo(path.getPath());
            path.setPathInfo(pathInfo);
        }
    }
}
