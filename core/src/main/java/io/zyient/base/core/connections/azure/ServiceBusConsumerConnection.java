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

package io.zyient.base.core.connections.azure;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.google.common.base.Preconditions;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.EMessageClientMode;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.azure.AzureServiceBusConnectionSettings;
import io.zyient.base.core.connections.settings.azure.QueueOrTopic;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class ServiceBusConsumerConnection extends ServiceBusConnection {
    private ServiceBusReceiverClient client;

    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            read(config, env);
            return setup(settings, env);
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            read(name, connection, path, env);
            return setup(settings, env);
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof AzureServiceBusConnectionSettings);
        this.env = env;
        this.settings = (AzureServiceBusConnectionSettings) settings;
        if (this.settings.getMode() != EMessageClientMode.Consumer) {
            throw new ConnectionError("Connection not configured as producer...");
        }
        state.setState(EConnectionState.Initialized);
        return this;
    }

    @Override
    public Connection connect() throws ConnectionError {
        Preconditions.checkState(settings instanceof AzureServiceBusConnectionSettings);
        try {
            synchronized (state) {
                if (!isConnected()) {
                    if (((AzureServiceBusConnectionSettings) settings).getQueueOrTopic() == QueueOrTopic.Queue) {
                        client = new ServiceBusClientBuilder()
                                .connectionString(((AzureServiceBusConnectionSettings) settings).getConnectionString())
                                .receiver()
                                .queueName(settings.getQueue())
                                .subscriptionName(env.moduleInstance().getInstanceId())
                                .buildClient();
                    } else {
                        client = new ServiceBusClientBuilder()
                                .connectionString(((AzureServiceBusConnectionSettings) settings).getConnectionString())
                                .receiver()
                                .topicName(settings.getQueue())
                                .subscriptionName(env.moduleInstance().getInstanceId())
                                .buildClient();
                    }
                }
            }
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public boolean canSend() {
        return false;
    }

    @Override
    public boolean canReceive() {
        return true;
    }

    @Override
    public void close() throws IOException {
        if (state.isConnected()) {
            state.setState(EConnectionState.Closed);
        }
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
