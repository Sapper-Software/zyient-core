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

package io.zyient.base.core.io.sync.local;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.StringListParser;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.base.core.io.sync.FileSystemSyncSettings;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
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
 *             <scanOnStart>[Sync file system on startup, default = false]</scanOnStart>
 *             <fullScanInterval>[Full file system scan interval, default = 6Hrs]</fullScanInterval>
 *             <filters>[File/Directory filters for sync. default=none]</filters>
 *         </sync>
 *     </fs>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class LocalFsSyncSettings extends FileSystemSyncSettings {
    @Config(name = "scanOnStart", required = false, type = Boolean.class)
    private boolean scanOnStart = false;
    @Config(name = "fullScanInterval", required = false, parser = TimeValueParser.class)
    private TimeUnitValue fullScanInterval = new TimeUnitValue(6, TimeUnit.HOURS);
    @Config(name = "filters", required = false, parser = StringListParser.class)
    private List<String> filters = null;
}
