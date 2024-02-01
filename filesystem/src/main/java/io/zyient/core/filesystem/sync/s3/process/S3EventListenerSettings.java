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

package io.zyient.core.filesystem.sync.s3.process;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.core.messaging.MessagingProcessorSettings;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class S3EventListenerSettings extends MessagingProcessorSettings {
    @Config(name = "handler.class", required = false, type = Class.class)
    private Class<? extends S3EventHandler> handler;
    @Config(name = "timeouts.read", required = false, parser = TimeValueParser.class)
    private TimeUnitValue readTimeout = new TimeUnitValue(500, TimeUnit.MILLISECONDS);

    public String threadName() {
        return String.format("%s::%s", S3EventListener.class.getSimpleName(), getName());
    }
}
