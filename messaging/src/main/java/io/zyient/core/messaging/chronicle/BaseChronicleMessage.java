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

@Getter
@Setter
@Accessors(fluent = true)
public class BaseChronicleMessage<M> extends ChronicleMessage<String, M> {
    public static final String HEADER_MESSAGE_QUEUE = "queue";
    public static final String HEADER_MESSAGE_KEY = "key";
    private long timestamp;
    private long size;

    public BaseChronicleMessage() {

    }

    public BaseChronicleMessage(@NonNull MessageEnvelop envelop) {
        id(envelop.id());
        correlationId(envelop.correlationId());
        mode(MessageObject.MessageMode.valueOf(envelop.mode()));
        key(envelop.key());
        timestamp = envelop.timestamp();
        size = envelop.size();
        queue(envelop.queue());
    }
}
