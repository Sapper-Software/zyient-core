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

package io.zyient.core.messaging.kafka.builders;

import com.google.common.base.Preconditions;
import io.zyient.base.core.connections.kafka.BasicKafkaProducerConnection;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.core.messaging.builders.MessageSenderBuilder;
import io.zyient.core.messaging.builders.MessageSenderSettings;
import io.zyient.core.messaging.kafka.BaseKafkaProducer;
import io.zyient.core.messaging.kafka.KafkaPartitioner;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

public class KafkaProducerBuilder<M> extends MessageSenderBuilder<String, M> {
    private final Class<? extends BaseKafkaProducer<M>> type;

    public KafkaProducerBuilder(@NonNull Class<? extends BaseKafkaProducer<M>> type,
                                @NonNull Class<? extends MessageSenderSettings> settingsType) {
        super(settingsType);
        this.type = type;
    }

    public KafkaProducerBuilder(@NonNull Class<? extends BaseKafkaProducer<M>> type) {
        super(KafkaProducerSettings.class);
        this.type = type;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BaseKafkaProducer<M> build(@NonNull MessageSenderSettings settings) throws Exception {
        Preconditions.checkNotNull(env());
        Preconditions.checkArgument(settings.getType() == EConnectionType.kafka);
        Preconditions.checkArgument(settings instanceof KafkaProducerSettings);
        BasicKafkaProducerConnection connection = env().connectionManager()
                .getConnection(settings.getConnection(), BasicKafkaProducerConnection.class);
        if (connection == null) {
            throw new Exception(
                    String.format("Kafka Producer connection not found. [name=%s]", settings.getConnection()));
        }
        if (!connection.isConnected()) {
            connection.connect();
        }
        BaseKafkaProducer<M> producer = type.getDeclaredConstructor().newInstance();
        producer.withConnection(connection);
        if (((KafkaProducerSettings) settings).getPartitioner() != null) {
            KafkaPartitioner<M> partitioner = (KafkaPartitioner<M>) ((KafkaProducerSettings) settings)
                    .getPartitioner()
                    .getDeclaredConstructor()
                    .newInstance();
            partitioner.init(config());
            producer.partitioner(partitioner);
        }
        producer.init();
        if (StringUtils.isNotBlank(settings.getQueue())) {
            producer.withTopic(settings.getQueue());
        }
        return producer;
    }
}
