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
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.connections.kafka.BasicKafkaConsumerConnection;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.state.OffsetStateManager;
import io.zyient.base.core.state.OffsetStateManagerSettings;
import io.zyient.core.messaging.builders.MessageReceiverBuilder;
import io.zyient.core.messaging.builders.MessageReceiverSettings;
import io.zyient.core.messaging.kafka.BaseKafkaConsumer;
import io.zyient.core.messaging.kafka.BaseKafkaProducer;
import io.zyient.core.messaging.kafka.KafkaStateManager;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class KafkaConsumerBuilder<M> extends MessageReceiverBuilder<String, M> {
    private final Class<? extends BaseKafkaConsumer<M>> type;

    protected KafkaConsumerBuilder(@NonNull Class<? extends BaseKafkaConsumer<M>> type) {
        super(KafkaReceiverSettings.class);
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    @Override
    public BaseKafkaConsumer<M> build(@NonNull MessageReceiverSettings settings) throws Exception {
        Preconditions.checkNotNull(env());
        Preconditions.checkArgument(settings.getType() == EConnectionType.kafka);
        BasicKafkaConsumerConnection connection = env().connectionManager()
                .getConnection(settings.getConnection(), BasicKafkaConsumerConnection.class);
        if (connection == null) {
            throw new Exception(
                    String.format("Kafka Consumer connection not found. [name=%s]", settings.getConnection()));
        }
        if (!connection.isConnected()) {
            connection.connect();
        }
        BaseKafkaConsumer<M> consumer = type.getDeclaredConstructor().newInstance();
        consumer.withConnection(connection);

        if (!Strings.isNullOrEmpty(settings.getOffsetManager())) {
            OffsetStateManager<?> offsetStateManager = env().stateManager()
                    .getOffsetManager(settings.getOffsetManager(), KafkaStateManager.class);
            if (offsetStateManager == null) {
                throw new Exception(
                        String.format("Kafka State manager not found. [name=%s]", settings.getOffsetManager()));
            }
            consumer.withOffsetStateManager(offsetStateManager);
        }
        if (settings.getReceiverTimeout().normalized() > 0) {
            consumer.withReceiveTimeout(settings.getReceiverTimeout().normalized());
        }
        if (ConfigReader.checkIfNodeExists(config, KafkaReceiverSettings.__CONFIG_PATH_ERRORS)) {
            HierarchicalConfiguration<ImmutableNode> ec
                    = config.configurationAt(KafkaReceiverSettings.__CONFIG_PATH_ERRORS);
            Class<? extends BaseKafkaProducer<M>> type
                    = (Class<? extends BaseKafkaProducer<M>>) ConfigReader.readAsClass(ec);
            KafkaProducerBuilder<M> builder = new KafkaProducerBuilder<>(type);
            BaseKafkaProducer<M> producer = (BaseKafkaProducer<M>) builder.build(ec);
            return (BaseKafkaConsumer<M>) consumer
                    .withErrorQueue(producer)
                    .init();
        }
        return (BaseKafkaConsumer<M>) consumer.init();
    }

    private void readOffsetManager(MessageReceiverSettings settings,
                                   BaseKafkaConsumer<M> consumer) throws Exception {
        if (ConfigReader.checkIfNodeExists(config, OffsetStateManagerSettings.__CONFIG_PATH)) {
            Class<? extends OffsetStateManager<?>> cls = OffsetStateManager.parseManagerType(config);
            OffsetStateManager<?> manager = cls
                    .getDeclaredConstructor()
                    .newInstance()
                    .init(config, env());
            consumer.withOffsetStateManager(manager);
        } else if (!Strings.isNullOrEmpty(settings.getOffsetManager())) {
            OffsetStateManager<?> offsetStateManager = env().stateManager()
                    .getOffsetManager(settings.getOffsetManager(), KafkaStateManager.class);
            if (offsetStateManager == null) {
                throw new Exception(
                        String.format("Kafka State manager not found. [name=%s]", settings.getOffsetManager()));
            }
            consumer.withOffsetStateManager(offsetStateManager);
        }
    }
}
