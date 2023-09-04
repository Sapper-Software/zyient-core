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

package io.zyient.base.core.utils;

import lombok.NonNull;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileSystemUtils {
    public static List<Path> list(@NonNull String path, @NonNull FileSystem fs) throws IOException {
        Path hp = new Path(path);
        if (fs.exists(hp)) {
            List<Path> paths = new ArrayList<>();
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(hp, true);
            if (files != null) {
                while (files.hasNext()) {
                    LocatedFileStatus file = files.next();
                    if (file.isFile()) {
                        paths.add(file.getPath());
                    }
                }
            }
            if (!paths.isEmpty()) return paths;
        }
        return null;
    }
}
