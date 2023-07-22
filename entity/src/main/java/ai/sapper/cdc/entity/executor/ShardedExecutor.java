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

package ai.sapper.cdc.entity.executor;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.cdc.entity.model.TransactionId;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Accessors(fluent = true)
public class ShardedExecutor<T extends TransactionId> implements Closeable {
    private final ProcessorState state = new ProcessorState();
    @Getter(AccessLevel.NONE)
    private final ReentrantLock __lock = new ReentrantLock();
    @Getter(AccessLevel.NONE)
    private final Map<Integer, ThreadPoolExecutor> executors = new HashMap<>();
    @Setter(AccessLevel.NONE)
    private BaseEnv<?> env;
    private ShardedExecutorSettings settings;

    public ShardedExecutor<T> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                   @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            this.env = env;
            ConfigReader reader = new ConfigReader(xmlConfig, SchedulerSettings.__CONFIG_PATH, ShardedExecutorSettings.class);
            reader.read();
            settings = (ShardedExecutorSettings) reader.settings();
            for (int ii = 0; ii < settings().getShards(); ii++) {
                ThreadPoolExecutor executor = new ThreadPoolExecutor(
                        settings.getCorePoolSize(),
                        settings.getMaxPoolSize(),
                        settings.getKeepAliveTime(),
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(settings.getQueueSize())
                );
                executors.put(ii, executor);
            }
            state.setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Throwable t) {
            state.error(t);
            throw new ConfigurationException(t);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (state.isRunning()) {
                state.setState(ProcessorState.EProcessorState.Stopped);
            }
            if (!executors.isEmpty()) {
                for (ThreadPoolExecutor executorService : executors.values()) {
                    try {
                        executorService.shutdown();
                        while (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                            DefaultLogger.info("Waiting another 5 seconds for the embedded engine to shut down");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        DefaultLogger.error(
                                String.format("Error terminating executors: [%s]", e.getLocalizedMessage()));
                    }
                }
                executors.clear();
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
        }
    }

}
