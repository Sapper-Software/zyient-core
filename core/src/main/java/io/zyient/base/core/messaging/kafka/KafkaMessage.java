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

package io.zyient.base.core.messaging.kafka;

import io.zyient.base.core.messaging.MessageObject;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

@Getter
@Setter
@Accessors(fluent = true)
public class KafkaMessage<K, V> extends MessageObject<K, V> {
    private int partition;
    private long offset;

    public KafkaMessage() {
    }

    public KafkaMessage(MessageObject<K, V> source) {
        super(source);
        if (source instanceof KafkaMessage<K, V>) {
            partition = ((KafkaMessage<K, V>) source).partition;
            offset = ((KafkaMessage<K, V>)  source).offset;
        }
    }

    public KafkaMessage(@NonNull ConsumerRecord<K, ?> record, @NonNull V value) {
        queue(record.topic());
        key(record.key());
        value(value);
        partition = record.partition();
        offset = record.offset();

        Header ih = record.headers().lastHeader(HEADER_MESSAGE_ID);
        if (ih != null && ih.value() != null) {
            String mid = new String(ih.value(), StandardCharsets.UTF_8);
            id(mid);
        } else {
            id(String.format("%s:%d:%d", record.topic(), record.partition(), record.offset()));
        }

        Header ch = record.headers().lastHeader(HEADER_CORRELATION_ID);
        if (ch != null && ch.value() != null) {
            String cid = new String(ch.value(), StandardCharsets.UTF_8);
            correlationId(cid);
        }
        Header mh = record.headers().lastHeader(HEADER_MESSAGE_MODE);
        if (mh != null && mh.value() != null) {
            String m = new String(mh.value(), StandardCharsets.UTF_8);
            mode(MessageMode.valueOf(m));
        }
    }
}
