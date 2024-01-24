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

package io.zyient.core.filesystem.sync;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *     <fs>
 *         <fileSystems>
 *             <fileSystem>
 *                 <type>[FS class]</type>
 *                 <name>[File System name, must be unique in a namespace]</name>
 *                 ...
 *             </fileSystem>
 *             ...
 *         </fileSystems>
 *         <sync>
 *             <class>File System syncer implementation class</class>
 *             <fileSystem>Associated file system name</fileSystem>
 *             <zkConnection>ZooKeeper connection name</zkConnection>
 *             <frequency>[Sync frequency, default=1min]</frequency>
 *         </sync>
 *     </fs>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FileSystemSyncSettings extends Settings {
    public static final String __CONFIG_PATH = "sync";

    @Config(name = "class", type = Class.class)
    private Class<? extends FileSystemSync> type;
    @Config(name = "fileSystem")
    private String fs;
    @Config(name = "zkConnection")
    private String zkConnection;
    @Config(name = "frequency", required = false, parser = TimeValueParser.class)
    private TimeUnitValue frequency = new TimeUnitValue(60 * 1000, TimeUnit.MILLISECONDS);
}
