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

package io.zyient.core.filesystem.impl.sftp;

import io.zyient.core.filesystem.impl.RemoteFileSystem;
import io.zyient.core.filesystem.impl.RemoteReader;
import io.zyient.core.filesystem.model.FileInode;
import lombok.NonNull;

import java.io.IOException;

public class SftpReader extends RemoteReader {
    private final SftpPathInfo pathInfo;

    protected SftpReader(@NonNull FileInode path,
                         @NonNull RemoteFileSystem fs) throws IOException {
        super(path, fs);
        if (path.getPathInfo() != null) {
            pathInfo = (SftpPathInfo) path.getPathInfo();
        } else {
            pathInfo = (SftpPathInfo) fs.parsePathInfo(path.getURI());
            path.setPathInfo(pathInfo);
        }
    }
}
