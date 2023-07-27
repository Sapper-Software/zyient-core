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

package ai.sapper.cdc.core.state;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.common.config.units.TimeUnitValue;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *      <state>
 *          <stateManagerClass>[State Manager class]</stateManagerClass>
 *          <connection>[ZooKeeper connection name]</connection>
 *          <locking> -- Optional
 *              <retry>[Lock retry count, default = 4]</retry>
 *              <timeout>[Lock timeout, default = 15sec</timeout>
 *          </locking>
 *          <offsets>
 *              <offsetManager>
 *                  ...
 *              </offsetManager>
 *              ...
 *          </offsets>
 *      </state>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class BaseStateManagerSettings extends Settings {
    public static final String __CONFIG_PATH = "state";
    public static final String __CONFIG_PATH_OFFSET_MANAGERS = "offsets";

    public static final class Constants {
        public static final short LOCK_RETRY_COUNT = 4;
        public static final long LOCK_TIMEOUT = 15000;

        public static final String CONFIG_ZK_BASE = "basePath";
        public static final String CONFIG_ZK_CONNECTION = "connection";
        public static final String CONFIG_LOCK_RETRY = "locking.retry";
        public static final String CONFIG_LOCK_TIMEOUT = "locking.timeout";
        public static final String CONFIG_SAVE_OFFSETS = "offsets.save";
    }

    @Config(name = Constants.CONFIG_ZK_BASE)
    private String basePath;
    @Config(name = Constants.CONFIG_ZK_CONNECTION)
    private String zkConnection;
    @Config(name = Constants.CONFIG_LOCK_RETRY, required = false, type = Short.class)
    private short lockRetryCount = Constants.LOCK_RETRY_COUNT;
    @Config(name = Constants.CONFIG_LOCK_TIMEOUT, required = false, type = Long.class)
    private TimeUnitValue lockTimeout = new TimeUnitValue(Constants.LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
    @Config(name = Constants.CONFIG_SAVE_OFFSETS, required = false, type = Boolean.class)
    private boolean saveOffsetManager = true;

    public BaseStateManagerSettings() {
    }

    public BaseStateManagerSettings(@NonNull Settings source) {
        super(source);
        Preconditions.checkArgument(source instanceof BaseStateManagerSettings);
        this.basePath = ((BaseStateManagerSettings) source).basePath;
        this.zkConnection = ((BaseStateManagerSettings) source).zkConnection;
        this.lockTimeout = ((BaseStateManagerSettings) source).lockTimeout;
        this.lockRetryCount = ((BaseStateManagerSettings) source).lockRetryCount;
        this.saveOffsetManager = ((BaseStateManagerSettings) source).saveOffsetManager;
    }
}
