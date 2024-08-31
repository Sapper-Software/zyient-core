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

package io.zyient.core.messaging.kafka;

import io.zyient.base.common.messaging.MessagingError;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.messaging.MessageReceiver;
import org.apache.kafka.common.TopicPartition;

import java.util.Set;

public abstract class BaseKafkaConsumer<M> extends AbstractBaseKafkaConsumer<M> {
    @Override
    public MessageReceiver<String, M> init() throws MessagingError {
        return super.init();
    }

    protected Set<TopicPartition> getAssignedTopicPartitions() throws MessagingError {
        Set<TopicPartition> partitions = consumer.consumer().assignment();
        if (partitions == null || partitions.isEmpty()) {
            throw new MessagingError(String.format("No assigned partitions found. [name=%s][topic=%s]",
                    consumer.name(), topic));
        }
        state().setState(ProcessorState.EProcessorState.Running);
        return partitions;
    }

}
