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

package io.zyient.core.filesystem;

import io.zyient.base.common.utils.PathUtils;
import io.zyient.core.filesystem.model.Container;
import lombok.NonNull;

public class PathsBuilder {
    private final Container container;
    private final String zkBasePath;
    private final String fsBasePath;


    public PathsBuilder(@NonNull Container container,
                        @NonNull String zkBasePath,
                        @NonNull String fsBasePath) {
        this.container = container;
        this.zkBasePath = zkBasePath;
        this.fsBasePath = fsBasePath;
    }

    public String buildDomainZkPath() {
        return new PathUtils.ZkPathBuilder(zkBasePath)
                .withPath(container.getDomain())
                .build();
    }

    public String buildDomainFsPath() {
        return PathUtils.formatPath(String.format("%s/%s", fsBasePath, container.getDomain()));
    }

    public String buildZkPath(@NonNull String path) {
        return new PathUtils.ZkPathBuilder(buildDomainZkPath())
                .withPath(path)
                .build();
    }

    public String buildFsPath(@NonNull String path) {
        return PathUtils.formatPath(String.format("%s/%s", buildDomainFsPath(), path));
    }

    public String relativeFsPath(@NonNull String path) {
        if (path.startsWith(fsBasePath)) {
            path = path.replace(fsBasePath, "");
        }
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }

    public String relativeZkPath(@NonNull String path) {
        if (path.startsWith(zkBasePath)) {
            path = path.replace(zkBasePath, "");
        }
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }
}
