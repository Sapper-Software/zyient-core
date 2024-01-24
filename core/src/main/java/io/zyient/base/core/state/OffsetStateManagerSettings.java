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

package io.zyient.base.core.state;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.base.core.model.ESettingsSource;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *     <offsetManager>
 *         <name>[Name]</name>
 *         <type>[Class extends OffsetStateManager]</type>
 *         <connection>ZooKeeper connection name</connection>
 *         <basePath>ZooKeeper base path</basePath>
 *         <locking>
 *             -- optional
 *             <retry>[Retry count]</retry>
 *             <timeout>Lock acquire timeout</timeout>
 *         </locking>
 *     </offsetManager>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class OffsetStateManagerSettings extends Settings {
    public static final String __CONFIG_PATH = "offsetManager";

    public static final class Constants {
        public static final short LOCK_RETRY_COUNT = 4;
        public static final long LOCK_TIMEOUT = 10000;

        public static final String CONFIG_NAME = "name";
        public static final String CONFIG_TYPE = "type";
        public static final String CONFIG_ZK_BASE = "basePath";
        public static final String CONFIG_ZK_CONNECTION = "connection";
        public static final String CONFIG_LOCK_RETRY = "locking.retry";
        public static final String CONFIG_LOCK_TIMEOUT = "locking.timeout";
    }

    @Config(name = Constants.CONFIG_NAME)
    private String name;
    @Config(name = Constants.CONFIG_TYPE, type = Class.class)
    private Class<? extends OffsetStateManager<?>> type;
    @Config(name = Constants.CONFIG_ZK_BASE, required = false)
    private String basePath;
    @Config(name = Constants.CONFIG_ZK_CONNECTION)
    private String zkConnection;
    @Config(name = Constants.CONFIG_LOCK_RETRY, required = false, type = Short.class)
    private short lockRetryCount = Constants.LOCK_RETRY_COUNT;
    @Config(name = Constants.CONFIG_LOCK_TIMEOUT, required = false, parser = TimeValueParser.class)
    private TimeUnitValue lockTimeout = new TimeUnitValue(Constants.LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
    @JsonIgnore
    private ESettingsSource source;

    public OffsetStateManagerSettings() {
        source = ESettingsSource.File;
    }

    public OffsetStateManagerSettings(@NonNull Settings source) {
        super(source);
        Preconditions.checkArgument(source instanceof OffsetStateManagerSettings);
        this.name = ((OffsetStateManagerSettings) source).name;
        this.type = ((OffsetStateManagerSettings) source).type;
        this.basePath = ((OffsetStateManagerSettings) source).basePath;
        this.zkConnection = ((OffsetStateManagerSettings) source).zkConnection;
        this.lockTimeout = ((OffsetStateManagerSettings) source).lockTimeout;
        this.lockRetryCount = ((OffsetStateManagerSettings) source).lockRetryCount;
        this.source = ((OffsetStateManagerSettings) source).source;
    }
}