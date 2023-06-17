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

package ai.sapper.cdc.core.messaging.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Getter
@Setter
@Accessors(fluent = true)
public class KafkaOffsetData {
    private final String key;
    private final TopicPartition partition;
    private final OffsetAndMetadata offset;

    public KafkaOffsetData(String key, ConsumerRecord<String, byte[]> record) {
        this.key = key;
        this.partition = new TopicPartition(record.topic(), record.partition());
        this.offset = new OffsetAndMetadata(record.offset(), String.format("[Key=%s]", key));
    }
}
