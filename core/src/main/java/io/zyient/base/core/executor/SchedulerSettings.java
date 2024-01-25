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

package io.zyient.base.core.executor;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public  class SchedulerSettings extends Settings {
    public static final String __CONFIG_PATH = "scheduler";

    public static class Constants {
        public static final String CONFIG_CORE_POOL_SIZE = "corePoolSize";
        public static final String CONFIG_MAX_POOL_SIZE = "maxPoolSize";
        public static final String CONFIG_KEEP_ALIVE_TIME = "keepAliveTime";
    }

    @Config(name = Constants.CONFIG_CORE_POOL_SIZE, required = false, type = Integer.class)
    private int corePoolSize = 4;
    @Config(name = Constants.CONFIG_MAX_POOL_SIZE, required = false, type = Integer.class)
    private int maxPoolSize = corePoolSize;
    @Config(name = Constants.CONFIG_KEEP_ALIVE_TIME, required = false, type = Long.class)
    private long keepAliveTime = 30000;
}