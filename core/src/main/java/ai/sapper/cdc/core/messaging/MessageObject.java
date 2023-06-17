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

package ai.sapper.cdc.core.messaging;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.UUID;

@Getter
@Setter
@Accessors(fluent = true)
public class MessageObject<K, V> {
    public enum MessageMode {
        New, ReSend, Snapshot, Backlog, Error, Retry, Forked, Recursive, Schema
    }

    public static final String HEADER_CORRELATION_ID = "CDC_CORRELATION_ID";
    public static final String HEADER_MESSAGE_ID = "CDC_MESSAGE_ID";
    public static final String HEADER_MESSAGE_MODE = "CDC_MESSAGE_MODE";

    private String queue;
    private String id;
    private String correlationId;
    private MessageMode mode;
    private K key;
    private V value;

    public MessageObject() {
        this.id = UUID.randomUUID().toString();
    }

    public MessageObject(@NonNull String id) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id));
        this.id = id;
    }

    public MessageObject(@NonNull MessageObject<K, V> source) {
        this.id = source.id;
    }
}
