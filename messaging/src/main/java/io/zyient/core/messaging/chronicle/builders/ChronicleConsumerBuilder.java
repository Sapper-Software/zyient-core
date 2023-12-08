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

package io.zyient.core.messaging.chronicle.builders;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.connections.chronicle.ChronicleConsumerConnection;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.state.OffsetStateManager;
import io.zyient.core.messaging.builders.MessageReceiverBuilder;
import io.zyient.core.messaging.builders.MessageReceiverSettings;
import io.zyient.core.messaging.chronicle.BaseChronicleConsumer;
import io.zyient.core.messaging.chronicle.BaseChronicleProducer;
import io.zyient.core.messaging.chronicle.ChronicleStateManager;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class ChronicleConsumerBuilder<M> extends MessageReceiverBuilder<String, M> {
    private final Class<? extends BaseChronicleConsumer<M>> type;

    public ChronicleConsumerBuilder(@NonNull Class<? extends BaseChronicleConsumer<M>> type,
                                    @NonNull Class<? extends MessageReceiverSettings> settingsType) {
        super(settingsType);
        this.type = type;
    }

    public ChronicleConsumerBuilder(@NonNull Class<? extends BaseChronicleConsumer<M>> type) {
        super(ChronicleConsumerSettings.class);
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    @Override
    public BaseChronicleConsumer<M> build(@NonNull MessageReceiverSettings settings) throws Exception {
        Preconditions.checkNotNull(env());
        Preconditions.checkArgument(settings.getType() == EConnectionType.chronicle);
        ChronicleConsumerConnection connection = env().connectionManager()
                .getConnection(settings.getConnection(), ChronicleConsumerConnection.class);
        if (connection == null) {
            throw new Exception(
                    String.format("Chronicle Consumer connection not found. [name=%s]", settings.getConnection()));
        }
        if (!connection.isConnected()) {
            connection.connect();
        }
        BaseChronicleConsumer<M> consumer = type.getDeclaredConstructor().newInstance();
        consumer.withConnection(connection);
        if (!Strings.isNullOrEmpty(settings.getOffsetManager())) {
            OffsetStateManager<?> offsetStateManager = env().stateManager()
                    .getOffsetManager(settings.getOffsetManager(), ChronicleStateManager.class);
            if (offsetStateManager == null) {
                throw new Exception(
                        String.format("Chronicle State manager not found. [name=%s]", settings.getOffsetManager()));
            }
            consumer.withOffsetStateManager(offsetStateManager);
        }
        if (settings.getReceiverTimeout().normalized() > 0) {
            consumer.withReceiveTimeout(settings.getReceiverTimeout().normalized());
        }
        if (ConfigReader.checkIfNodeExists(config, ChronicleConsumerSettings.__CONFIG_PATH_ERRORS)) {
            HierarchicalConfiguration<ImmutableNode> ec
                    = config.configurationAt(ChronicleConsumerSettings.__CONFIG_PATH_ERRORS);
            Class<? extends BaseChronicleProducer<M>> type
                    = (Class<? extends BaseChronicleProducer<M>>) ConfigReader.readAsClass(ec);
            ChronicleProducerBuilder<M> builder = new ChronicleProducerBuilder<>(type);
            BaseChronicleProducer<M> producer = (BaseChronicleProducer<M>) builder.build(ec);
            return (BaseChronicleConsumer<M>) consumer
                    .withErrorQueue(producer)
                    .init();
        }
        return (BaseChronicleConsumer<M>) consumer.init();
    }
}
