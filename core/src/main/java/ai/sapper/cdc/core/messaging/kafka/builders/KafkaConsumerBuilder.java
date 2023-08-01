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

package ai.sapper.cdc.core.messaging.kafka.builders;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.core.connections.kafka.BasicKafkaConsumerConnection;
import ai.sapper.cdc.core.connections.settings.EConnectionType;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverBuilder;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverSettings;
import ai.sapper.cdc.core.messaging.kafka.BaseKafkaConsumer;
import ai.sapper.cdc.core.messaging.kafka.KafkaStateManager;
import ai.sapper.cdc.core.state.OffsetStateManager;
import ai.sapper.cdc.core.state.OffsetStateManagerSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;

public class KafkaConsumerBuilder<M> extends MessageReceiverBuilder<String, M> {
    private final Class<? extends BaseKafkaConsumer<M>> type;

    protected KafkaConsumerBuilder(@NonNull Class<? extends BaseKafkaConsumer<M>> type,
                                   @NonNull Class<? extends MessageReceiverSettings> settingsType) {
        super(settingsType);
        this.type = type;
    }

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
        if (env().auditLogger() != null) {
            consumer.withAuditLogger(env().auditLogger());
        }
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
