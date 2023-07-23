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

package ai.sapper.cdc.core.executor;

import ai.sapper.cdc.common.config.Config;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ShardedExecutorSettings extends SchedulerSettings {
    public static final String __CONFIG_PATH = "executor";

    public static class Constants {
        public static final String CONFIG_SHARDS = "shards";
        public static final String CONFIG_QUEUE_SIZE = "queueSize";
    }

    @Config(name = Constants.CONFIG_SHARDS, required = false, type = Integer.class)
    private int shards = 1;
    @Config(name = Constants.CONFIG_QUEUE_SIZE, required = false, type = Integer.class)
    private int queueSize = 128;
}