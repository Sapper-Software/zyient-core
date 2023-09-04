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

package io.zyient.base.core.messaging.builders;

import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import io.zyient.base.core.connections.settings.EConnectionType;
import lombok.Getter;
import lombok.Setter;

/**
 * <pre>
 *     <type>[EConnectionType]</type>
 *     <connection>[Message connection name]</connection>
 * </pre>
 */
@Getter
@Setter
public class MessageSenderSettings extends Settings {
    @Config(name = "type", type = EConnectionType.class)
    private EConnectionType type;
    @Config(name = "connection")
    private String connection;
}
