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

package io.zyient.base.core.connections.kafka;

import com.google.common.base.Preconditions;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.EMessageClientMode;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.kafka.KafkaSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.kafka.clients.producer.KafkaProducer;

import javax.naming.ConfigurationException;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class KafkaProducerConnection<K, V> extends KafkaConnection {
    private KafkaProducer<K, V> producer;

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            super.init(xmlConfig, env);
            try {
                setup();
                state.setState(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening Kafka connection.", t);
            }
        }
        return this;
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            super.init(name, connection, path, env);
            try {
                setup();
                state.setState(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening Kafka connection.", t);
            }
        }
        return this;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            super.setup(settings, env);
            try {
                setup();
                state.setState(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening Kafka connection.", t);
            }
        }
        return this;
    }

    private void setup() throws Exception {
        if (settings().getMode() != EMessageClientMode.Producer) {
            throw new ConfigurationException("Connection not initialized in Producer mode.");
        }
        settings().setConnectionClass(getClass());
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection connect() throws ConnectionError {
        Preconditions.checkState(settings instanceof KafkaSettings);
        synchronized (state) {
            Preconditions.checkState(connectionState() == EConnectionState.Initialized);
            if (!state.isConnected()) {
                try {
                    producer = new KafkaProducer<K, V>(((KafkaSettings) settings).getProperties());

                    state.setState(EConnectionState.Connected);
                } catch (Throwable t) {
                    state.error(t);
                    throw new ConnectionError("Error opening HDFS connection.", t);
                }
            }
        }
        return this;
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        synchronized (state) {
            if (state.isConnected()) {
                state.setState(EConnectionState.Closed);
            }
            if (producer != null) {
                producer.close();
                producer = null;
            }
        }
    }

    /**
     * @return
     */
    @Override
    public boolean canSend() {
        return true;
    }

    /**
     * @return
     */
    @Override
    public boolean canReceive() {
        return false;
    }
}
