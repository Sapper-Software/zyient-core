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

package io.zyient.base.core.connections;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class MessageConnectionSettings extends ConnectionSettings {
    public static class Constants {
        public static final String CONFIG_QUEUE_NAME = "queue";
        public static final String CONFIG_BATCH_SIZE = "batchSize";
    }

    @Config(name = Constants.CONFIG_QUEUE_NAME)
    private String queue;
    @Config(name = Constants.CONFIG_BATCH_SIZE, required = false, type = Integer.class)
    private int batchSize = 128;
    @Config(name = EMessageClientMode.CONFIG_MODE, required = false, type = EMessageClientMode.class)
    private EMessageClientMode mode = EMessageClientMode.Producer;

    protected MessageConnectionSettings(@NonNull EConnectionType type) {
        setType(type);
    }

    protected MessageConnectionSettings(@NonNull MessageConnectionSettings settings) {
        super(settings);
        queue = settings.queue;
        batchSize = settings.batchSize;
        mode = settings.mode;
    }
}
