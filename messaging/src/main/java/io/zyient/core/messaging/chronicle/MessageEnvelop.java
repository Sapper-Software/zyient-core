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

import io.zyient.core.messaging.MessageObject;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

@Getter
@Setter
@Accessors(fluent = true)
public class MessageEnvelop extends SelfDescribingMarshallable {
    private String id;
    private String correlationId;
    private String mode;
    private String key;
    private String queue;
    private long timestamp;
    private long size;
    private byte[] data;

    public MessageEnvelop() {

    }

    public MessageEnvelop(@NonNull MessageObject<String, ?> message,
                          byte[] data) {
        id = message.id();
        correlationId = message.correlationId();
        mode = message.mode().name();
        key = message.key();
        timestamp = System.nanoTime();
        size = data.length;
        this.data = data;
    }
}
