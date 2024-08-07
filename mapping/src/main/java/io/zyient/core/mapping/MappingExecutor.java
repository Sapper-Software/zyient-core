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

package io.zyient.core.mapping;

import com.google.common.base.Preconditions;
import io.zyient.base.common.StateException;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.ConnectionManager;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.pipeline.Pipeline;
import io.zyient.core.mapping.pipeline.PipelineBuilder;
import io.zyient.core.mapping.pipeline.PipelineHandle;
import io.zyient.core.mapping.pipeline.PipelineSource;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadResponse;
import io.zyient.core.persistence.DataStoreManager;
import io.zyient.core.persistence.env.DataStoreEnv;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Getter
@Accessors(fluent = true)
public class MappingExecutor implements Closeable {
    public static final String __CONFIG_PATH_PIPELINE = "executor";
    public static final String __CONFIG_PATH_EXECUTOR = "settings";

    private final ProcessorState state = new ProcessorState();
    private PipelineBuilder builder;
    private MappingExecutorSettings settings;
    private ExecutorService executorService;
    private DataStoreManager dataStoreManager;
    private ConnectionManager connectionManager;
    private DataStoreEnv<?> env;
    private File mappingFile;

    public MappingExecutor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                @NonNull BaseEnv<?> env) throws ConfigurationException {
        Preconditions.checkArgument(env instanceof DataStoreEnv<?>);
        this.env = (DataStoreEnv<?>) env;
        this.dataStoreManager = ((DataStoreEnv<?>) env).getDataStoreManager();
        this.connectionManager = dataStoreManager.connectionManager();
        try {
            HierarchicalConfiguration<ImmutableNode> config = xmlConfig;
            if (config.getRootElementName().compareTo(__CONFIG_PATH_PIPELINE) != 0) {
                config = xmlConfig.configurationAt(__CONFIG_PATH_PIPELINE);
            }
            ConfigReader reader = new ConfigReader(config,
                    __CONFIG_PATH_EXECUTOR,
                    MappingExecutorSettings.class);
            reader.read();
            settings = (MappingExecutorSettings) reader.settings();
            executorService = new ThreadPoolExecutor(settings.getNumThreads(),
                    settings.getNumThreads(),
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(settings.getTaskQueueSize()));
            state.setState(ProcessorState.EProcessorState.Running);

            builder = new PipelineBuilder()
                    .withMappingFile(mappingFile)
                    .configure(config, env);

            __instance = this;

            return this;
        } catch (Exception ex) {
            state.error(ex);
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    private void checkState() throws StateException {
        if (!state.isRunning()) {
            throw new StateException(String.format("Executor not available. [state=%s]", state.getState().name()));
        }
    }

    public void read(@NonNull InputContentInfo contentInfo) throws Exception {
        checkState();
        PipelineHandle handle = builder.buildInputPipeline(contentInfo);
        if (handle == null) {
            throw new Exception(DefaultLogger.traceInfo("Failed to get pipeline.", contentInfo));
        }
        if (handle.pipeline() == null) {
            throw new Exception("Failed to get pipeline...");
        }
        if (handle.reader() == null) {
            throw new Exception("Failed to get input reader...");
        }
        Reader reader = new Reader(handle.pipeline(),
                handle.reader(),
                contentInfo);
        executorService.submit(reader);
    }

    @Override
    public void close() throws IOException {
        if (state.isRunning() || state.isAvailable()) {
            state.setState(ProcessorState.EProcessorState.Stopped);
        }
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
            executorService = null;
        }
        __instance = null;
    }

    @Getter(AccessLevel.NONE)
    private static MappingExecutor __instance;

    public static MappingExecutor defaultInstance() throws Exception {
        Preconditions.checkNotNull(__instance);
        __instance.checkState();
        return __instance;
    }

    public static MappingExecutor create(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                         @NonNull BaseEnv<?> env) throws Exception {
        return new MappingExecutor().init(xmlConfig, env);
    }

    public static MappingExecutor createWithFile(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                 @NonNull BaseEnv<?> env, File file) throws Exception {
        MappingExecutor executor = new MappingExecutor();
        executor.mappingFile = file;
        return executor.init(xmlConfig, env);
    }

    private record Reader(Pipeline pipeline,
                          InputReader reader,
                          InputContentInfo contentInfo) implements Runnable {

        @Override
        public void run() {
            try {
                if (!(pipeline instanceof PipelineSource)) {
                    throw new Exception(String.format("Invalid pipeline, not a source pipeline. [name=%s][type=%s]",
                            pipeline.name(), pipeline.getClass().getCanonicalName()));
                }
                DefaultLogger.info(String.format("Starting pipeline. [name=%s]", pipeline.name()));
                DefaultLogger.trace(pipeline.name(), contentInfo);

                ReadResponse response = ((PipelineSource) pipeline).read(reader, contentInfo);
                if (contentInfo.callback() != null) {
                    contentInfo.callback().onSuccess(contentInfo, response);
                }
                DefaultLogger.info(String.format("Finished pipeline. [name=%s]", pipeline.name()));
            } catch (Throwable ex) {
                String mesg = String.format("Pipeline failed: [error=%s][pipeline=%s]",
                        ex.getLocalizedMessage(), pipeline.name());
                DefaultLogger.stacktrace(ex);
                DefaultLogger.error(mesg);
                if (contentInfo.callback() != null) {
                    contentInfo.callback().onError(contentInfo, ex);
                }
            }
        }
    }
}
