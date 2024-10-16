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

package io.zyient.core.filesystem.impl.azure;

import io.zyient.core.filesystem.impl.RemoteWriter;
import io.zyient.core.filesystem.model.FileInode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class AzureWriter extends RemoteWriter {
    private final AzurePathInfo pathInfo;

    protected AzureWriter(@NonNull FileInode path,
                          @NonNull AzureFileSystem fs,
                          boolean overwrite) throws IOException {
        super(path, fs, overwrite);
        if (path.getPathInfo() != null) {
            pathInfo = (AzurePathInfo) path.getPathInfo();
        } else {
            pathInfo = (AzurePathInfo) fs.parsePathInfo(path.getURI());
            path.setPathInfo(pathInfo);
        }
    }

    protected AzureWriter(@NonNull FileInode path,
                          @NonNull AzureFileSystem fs,
                          @NonNull File temp) throws IOException {
        super(path, fs, temp);
        if (path.getPathInfo() != null) {
            pathInfo = (AzurePathInfo) path.getPathInfo();
        } else {
            pathInfo = (AzurePathInfo) fs.parsePathInfo(path.getURI());
            path.setPathInfo(pathInfo);
        }
    }

    @Override
    protected String getTmpPath() {
        String dir = FilenameUtils.getPath(pathInfo.path());
        return String.format("%s/%s", pathInfo.container(), dir);
    }
}
