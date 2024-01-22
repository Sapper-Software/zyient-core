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

package io.zyient.core.messaging.builders;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.base.core.connections.settings.EConnectionType;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *     <receiver> -- Or root name
 *         <type>[EConnectionType]</type>
 *         <connection>[Message Connection name]</connection>
 *         <offset>
 *             <manager>[Offset Manager name]</manager>
 *         </offset>
 *         <batchSize>[Receive batch size, default = -1(ignore)]</batchSize>
 *         <receiverTimeout>[Receiver timeout, default = -1(ignore)]</receiverTimeout>
 *         <errorQueue>
 *             <class>[Chronicle Producer implementation class]</class>
 *             -- Chronicle Producer settings --
 *             <type>[EConnectionType]</type>
 *             ...
 *         </errorQueue>
 *     </receiver>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class MessageReceiverSettings extends Settings {
    public static final String __CONFIG_PATH_ERRORS = "errorQueue";
    @Config(name = "type", type = EConnectionType.class)
    private EConnectionType type;
    @Config(name = "connection")
    private String connection;
    @Config(name = "offset.manager", required = false)
    private String offsetManager;
    @Config(name = "batchSize", required = false, type = Integer.class)
    private int batchSize = 1024;
    @Config(name = "receiverTimeout", required = false, parser = TimeValueParser.class)
    private TimeUnitValue receiverTimeout = new TimeUnitValue(30 * 1000, TimeUnit.MILLISECONDS);
}
