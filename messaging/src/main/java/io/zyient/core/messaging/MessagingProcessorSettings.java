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

package io.zyient.core.messaging;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.base.core.processing.ProcessorSettings;
import io.zyient.core.messaging.builders.MessageReceiverBuilder;
import io.zyient.core.messaging.builders.MessageReceiverSettings;
import io.zyient.core.messaging.builders.MessageSenderBuilder;
import io.zyient.core.messaging.builders.MessageSenderSettings;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *     <processor @type="[Processor implementation]">
 *         <name>[Processor Name]</name>
 *         <queue>
 *              <builder>
 *                  <type>[Message Builder class]</type>
 *                  <settingsType>[Message Builder Settings class]</settingsType>
 *                  <queue>[Queue Configuration]</queue>
 *              </builder>
 *              <receiver>
 *                  [Receiver settings]
 *                  ...
 *              </receiver>
 *         </queue>
 *     </processor>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class MessagingProcessorSettings extends ProcessorSettings {
    public static class Constants {
        public static final String __CONFIG_PATH_RECEIVER = "queue";
        public static final String __CONFIG_PATH_ERRORS = "errors";

        public static final String CONFIG_BUILDER_TYPE = "queue.builder.type";
        public static final String CONFIG_MESSAGING_SETTINGS_TYPE = "queue.builder.settingsType";
        public static final String CONFIG_ERRORS_BUILDER_TYPE = "errors.builder.type";
        public static final String CONFIG_ERRORS_MESSAGING_SETTINGS_TYPE = "errors.builder.settingsType";
        public static final String CONFIG_BATCH_RECEIVE_TIMEOUT = "readBatchTimeout";
    }

    @Config(name = Constants.CONFIG_BUILDER_TYPE, type = Class.class)
    private Class<? extends MessageReceiverBuilder<?, ?>> builderType;
    @Config(name = Constants.CONFIG_MESSAGING_SETTINGS_TYPE, type = Class.class)
    private Class<? extends MessageReceiverSettings> builderSettingsType;
    @Config(name = Constants.CONFIG_BATCH_RECEIVE_TIMEOUT, required = false, parser = TimeValueParser.class)
    private TimeUnitValue receiveBatchTimeout = new TimeUnitValue(1000, TimeUnit.MILLISECONDS);
    @Config(name = Constants.CONFIG_ERRORS_BUILDER_TYPE, required = false, type = Class.class)
    private Class<? extends MessageSenderBuilder<?, ?>> errorsBuilderType;
    @Config(name = Constants.CONFIG_ERRORS_MESSAGING_SETTINGS_TYPE, required = false, type = Class.class)
    private Class<? extends MessageSenderSettings> errorsBuilderSettingsType;
}
