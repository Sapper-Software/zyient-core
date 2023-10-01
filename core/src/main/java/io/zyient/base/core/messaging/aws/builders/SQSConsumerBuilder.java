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

package io.zyient.base.core.messaging.aws.builders;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.connections.aws.AwsSQSConsumerConnection;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.messaging.aws.AwsSQSStateManager;
import io.zyient.base.core.messaging.aws.BaseSQSConsumer;
import io.zyient.base.core.messaging.aws.BaseSQSProducer;
import io.zyient.base.core.messaging.builders.MessageReceiverBuilder;
import io.zyient.base.core.messaging.builders.MessageReceiverSettings;
import io.zyient.base.core.state.OffsetStateManager;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class SQSConsumerBuilder<M> extends MessageReceiverBuilder<String, M> {
    private final Class<? extends BaseSQSConsumer<M>> type;

    public SQSConsumerBuilder(@NonNull Class<? extends MessageReceiverSettings> settingsType,
                              @NonNull Class<? extends BaseSQSConsumer<M>> type) {
        super(settingsType);
        this.type = type;
    }

    public SQSConsumerBuilder(@NonNull Class<? extends BaseSQSConsumer<M>> type) {
        super(SQSConsumerSettings.class);
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    @Override
    public BaseSQSConsumer<M> build(@NonNull MessageReceiverSettings settings) throws Exception {
        Preconditions.checkNotNull(env());
        Preconditions.checkArgument(settings.getType() == EConnectionType.sqs);
        AwsSQSConsumerConnection connection = env().connectionManager()
                .getConnection(settings.getConnection(), AwsSQSConsumerConnection.class);
        if (connection == null) {
            throw new Exception(
                    String.format("SQS Consumer connection not found. [name=%s]", settings.getConnection()));
        }
        if (!connection.isConnected()) {
            connection.connect();
        }
        BaseSQSConsumer<M> consumer = type.getDeclaredConstructor().newInstance();
        consumer.withConnection(connection);
        if (!Strings.isNullOrEmpty(settings.getOffsetManager())) {
            OffsetStateManager<?> offsetStateManager = env().stateManager()
                    .getOffsetManager(settings.getOffsetManager(), AwsSQSStateManager.class);
            if (offsetStateManager == null) {
                throw new Exception(
                        String.format("SQS State manager not found. [name=%s]", settings.getOffsetManager()));
            }
            consumer.withOffsetStateManager(offsetStateManager);
        }
        if (ConfigReader.checkIfNodeExists(config, SQSConsumerSettings.__CONFIG_PATH_ERRORS)) {
            HierarchicalConfiguration<ImmutableNode> ec
                    = config.configurationAt(SQSConsumerSettings.__CONFIG_PATH_ERRORS);
            Class<? extends BaseSQSProducer<M>> type
                    = (Class<? extends BaseSQSProducer<M>>) ConfigReader.readAsClass(ec);
            SQSProducerBuilder<M> builder = new SQSProducerBuilder<>(type);
            BaseSQSProducer<M> producer = (BaseSQSProducer<M>) builder.build(ec);
            return (BaseSQSConsumer<M>) consumer
                    .withErrorQueue(producer)
                    .init();
        }
        return (BaseSQSConsumer<M>) consumer.init();
    }
}
