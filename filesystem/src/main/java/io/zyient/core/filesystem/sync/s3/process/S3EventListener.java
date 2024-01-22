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

package io.zyient.core.filesystem.sync.s3.process;

import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.threads.ManagedThread;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.RunUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.aws.AwsSQSConsumerConnection;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.filesystem.sync.s3.model.S3Event;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class S3EventListener implements Closeable {
    private final ProcessorState state = new ProcessorState();
    private S3EventHandler handler = null;
    private S3EventListenerSettings settings;
    private BaseEnv<?> env;
    private AwsSQSConsumerConnection connection;
    private S3EventConsumer consumer;
    private ManagedThread executor;

    public S3EventListener withHandler(@NonNull S3EventHandler handler) {
        this.handler = handler;
        return this;
    }

    public S3EventListener init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                @NonNull BaseEnv<?> env) throws ConfigurationException {
        this.env = env;
        try {
            ConfigReader reader = new ConfigReader(config,
                    S3EventListenerSettings.class);
            reader.read();
            settings = (S3EventListenerSettings) reader.settings();
            connection = env.connectionManager()
                    .getConnection(settings.getConnection(), AwsSQSConsumerConnection.class);
            if (connection == null) {
                throw new Exception(String.format("SQS Connection not found. [name=%s][type=%s]",
                        settings.getConnection(), AwsSQSConsumerConnection.class.getCanonicalName()));
            }
            consumer = new S3EventConsumer(connection,
                    settings.getQueue(),
                    settings().getBatchSize(),
                    settings.getReadTimeout().normalized(),
                    settings.getAckTimeout().normalized().intValue());
            if (settings.getHandler() != null) {
                handler = settings.getHandler()
                        .getDeclaredConstructor()
                        .newInstance();
                handler.init(reader.config(), env);
            } else if (handler == null) {
                throw new Exception("Event handler not defined...");
            }
            state.setState(ProcessorState.EProcessorState.Running);
            Runner runner = new Runner(this, consumer);
            executor = new ManagedThread(env, runner, settings.threadName());
            env.addThread(executor.getName(), executor);
            executor.start();
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        if (state.isAvailable()) {
            state.setState(ProcessorState.EProcessorState.Stopped);
        }
        try {
            if (executor != null) {
                executor.close();
                executor.join();
                env.removeThread(executor.getName());
                executor = null;
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new IOException(ex);
        }
    }

    public static class Runner implements Runnable {
        private final S3EventListener parent;
        private final S3EventConsumer consumer;

        public Runner(S3EventListener parent, S3EventConsumer consumer) {
            this.parent = parent;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            try {
                while (parent.state.isRunning()) {
                    List<S3Event> events = consumer.next();
                    if (events == null || events.isEmpty()) {
                        RunUtils.sleep(parent.settings.getReadTimeout().normalized());
                        continue;
                    }
                    for (S3Event event : events) {
                        parent.handler.handle(event);
                        consumer.ack(event);
                    }
                    consumer.commit();
                }
            } catch (RuntimeException re) {
                DefaultLogger.error("S3 Event listener stopped.", re);
                DefaultLogger.stacktrace(re);
                parent.state.error(re);
            } catch (Throwable t) {
                DefaultLogger.error("S3 Event listener stopped.", t);
                DefaultLogger.stacktrace(t);
                parent.state.error(t);
            }
        }
    }
}
