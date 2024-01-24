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

package io.zyient.core.filesystem.indexing;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.Settings;
import lombok.Getter;
import lombok.Setter;

/**
 * <pre>
 *     <fs>
 *         <fileSystems>
 *             <fileSystem>
 *                 <type>[FS class]</type>
 *                 <name>[File System name, must be unique in a namespace]</name>
 *                 ...
 *                 <indexer>
 *                     <directory>[Path to index storage]</directory>
 *                     <mapped>[Used Memory mapped index. default=true]</mapped>
 *                     <poolSize>[Executor pool size, default=4]</poolSize>
 *                 </indexer>
 *             </fileSystem>
 *             ...
 *         </fileSystems>
 *     </fs>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@ConfigPath(path = "indexer")
public class FileSystemIndexerSettings extends Settings {
    @Config(name = "directory")
    private String directory;
    @Config(name = "connection")
    private String zkConnection;
    @Config(name = "mapped", required = false, type = Boolean.class)
    private boolean useMappedFiles = true;
    @Config(name = "poolSize", required = false, type = Integer.class)
    private int poolSize = 4;

    public static String path() {
        ConfigPath cp = FileSystemIndexerSettings.class.getAnnotation(ConfigPath.class);
        return cp.path();
    }
}
