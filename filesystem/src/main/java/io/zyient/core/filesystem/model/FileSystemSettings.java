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

package io.zyient.core.filesystem.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.base.core.model.ESettingsSource;
import io.zyient.core.filesystem.indexing.FileSystemIndexerSettings;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *     <fs>
 *         <fileSystems>
 *             <fileSystem>
 *                 <type>[FS class]</type>
 *                 <name>[File System name, must be unique in a namespace]</name>
 *                 <tmp>
 *                     <path>[temporary storage path (optional)</path>
 *                     <clean>[true|false, default=true]</clean>
 *                     <ttl>[Clean files after, in milliseconds]</ttl>
 *                 </tmp>
 *                 <zk> -- Optional
 *                     <connection>[zookeeper connection name]</connection>
 *                     <path>[zookeeper path]</path>
 *                     <lockTimeout>[distributed lock timeout (optional)</lockTimeout>
 *                 </zk>
 *                 <compressed>[true|false, default=false]</compressed>
 *                 <encryption>
 *                     <enable>[true/false, default=false]</enable>
 *                     <key>[Encryption Key reference]</key>
 *                 </encryption>
 *                 <containers>
 *                     <container>
 *                         ...
 *                     </container>
 *                     ...
 *                     <default>[Default domain]</default>
 *                 </containers>
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
public abstract class FileSystemSettings extends Settings {
    public static final String TEMP_PATH = String.format("%s/zyient/cdc",
            System.getProperty("java.io.tmpdir"));

    public static class Constants {
        public static final String CONFIG_FS_CLASS = "type";
        public static final String CONFIG_NAME = "name";
        public static final String CONFIG_BASE_PATH = "basePath";
        public static final String CONFIG_TEMP_FOLDER = "tmp.path";
        public static final String CONFIG_TEMP_TTL = "tmp.ttl";
        public static final String CONFIG_TEMP_CLEAN = "tmp.clean";
        public static final String CONFIG_TEMP_CLEAN_INTERVAL = "tmp.interval";
        public static final String CONFIG_ZK_CONNECTION = "zk.connection";
        public static final String CONFIG_ZK_PATH = "zk.path";
        public static final String CONFIG_ZK_LOCK_TIMEOUT = "zk.lockTimeout";
        public static final String CONFIG_DEFAULT_COMPRESSED = "compressed";
        public static final String CONFIG_ENCRYPTED = "encryption.enable";
        public static final String CONFIG_ENCRYPTION_KEY = "encryption.key";
        public static final int LOCK_TIMEOUT = 60 * 1000;
    }

    @Config(name = Constants.CONFIG_FS_CLASS)
    private String type;
    @Config(name = Constants.CONFIG_NAME)
    private String name;
    @Config(name = Constants.CONFIG_BASE_PATH)
    private String basePath;
    @Config(name = Constants.CONFIG_ZK_CONNECTION, required = false)
    private String zkConnection;
    @Config(name = Constants.CONFIG_ZK_PATH, required = false)
    private String zkPath;
    @Config(name = Constants.CONFIG_TEMP_FOLDER, required = false)
    private String tempDir = TEMP_PATH;
    @Config(name = Constants.CONFIG_TEMP_CLEAN, required = false, type = Boolean.class)
    private boolean cleanTmp = true;
    @Config(name = Constants.CONFIG_TEMP_TTL, required = false, parser = TimeValueParser.class)
    private TimeUnitValue tempTTL = new TimeUnitValue(30L * 60 * 1000, TimeUnit.MILLISECONDS);
    @Config(name = Constants.CONFIG_TEMP_CLEAN_INTERVAL, required = false, parser = TimeValueParser.class)
    private TimeUnitValue tempCleanInterval = new TimeUnitValue(60 * 1000, TimeUnit.MILLISECONDS);
    @Config(name = Constants.CONFIG_ZK_LOCK_TIMEOUT, required = false, parser = TimeValueParser.class)
    private TimeUnitValue lockTimeout = new TimeUnitValue(Constants.LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
    @Config(name = Constants.CONFIG_DEFAULT_COMPRESSED, required = false, type = Boolean.class)
    private boolean compressed = false;
    @Config(name = Constants.CONFIG_ENCRYPTED, required = false, type = Boolean.class)
    private boolean encrypted = false;
    @Config(name = Constants.CONFIG_ENCRYPTION_KEY, required = false)
    private String encryptionKey;
    private Container defaultContainer;
    private Map<String, Container> containers;
    private ESettingsSource source = ESettingsSource.File;
    private FileSystemIndexerSettings indexerSettings;
}
