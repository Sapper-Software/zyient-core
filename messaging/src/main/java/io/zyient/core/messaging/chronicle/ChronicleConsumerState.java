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

package io.zyient.core.messaging.chronicle;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.state.OffsetState;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ChronicleConsumerState extends OffsetState<Connection.EConnectionState, ChronicleOffset> {
    public static final String OFFSET_TYPE = "chronicle/consumer";

    private String queue;

    public ChronicleConsumerState() {
        super(Connection.EConnectionState.Error, Connection.EConnectionState.Initialized, OFFSET_TYPE);
    }
}