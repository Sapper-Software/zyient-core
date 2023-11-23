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

package io.zyient.base.core.mapping;

import com.google.common.base.Preconditions;
import io.zyient.base.common.StateException;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.mapping.model.InputContentInfo;
import io.zyient.base.core.mapping.pipeline.PipelineBuilder;
import io.zyient.base.core.mapping.pipeline.PipelineHandle;
import io.zyient.base.core.mapping.pipeline.TransformerPipeline;
import io.zyient.base.core.mapping.readers.InputReader;
import io.zyient.base.core.mapping.readers.ReadResponse;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.stores.DataStoreManager;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Getter
@Accessors(fluent = true)
public class MappingExecutor implements Closeable {
    public static final String __CONFIG_PATH = "pipeline.executor";

    private final ProcessorState state = new ProcessorState();
    private PipelineBuilder builder;
    private MappingExecutorSettings settings;
    private ExecutorService executorService;
    private DataStoreManager dataStoreManager;

    public MappingExecutor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                @NonNull DataStoreManager dataStoreManager) throws ConfigurationException {
        this.dataStoreManager = dataStoreManager;
        try {
            HierarchicalConfiguration<ImmutableNode> config = xmlConfig;
            if (config.getRootElementName().compareTo(__CONFIG_PATH) != 0) {
                config = xmlConfig.configurationAt(__CONFIG_PATH);
            }
            ConfigReader reader = new ConfigReader(config, null, MappingExecutorSettings.class);
            reader.read();
            settings = (MappingExecutorSettings) reader.settings();
            builder = new PipelineBuilder()
                    .configure(config, dataStoreManager);
            executorService = new ThreadPoolExecutor(settings.getNumThreads(),
                    settings.getNumThreads(),
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(settings.getTaskQueueSize()));
            state.setState(ProcessorState.EProcessorState.Running);

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
        PipelineHandle<?, ?> handle = builder.buildInputPipeline(contentInfo);
        if (handle == null) {
            throw new Exception(DefaultLogger.traceInfo("Failed to get pipeline.", contentInfo));
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

    private static class Reader implements Runnable {
        private final TransformerPipeline<?, ?> pipeline;
        private final InputReader reader;
        private final InputContentInfo contentInfo;

        private Reader(TransformerPipeline<?, ?> pipeline,
                       InputReader reader,
                       InputContentInfo contentInfo) {
            this.pipeline = pipeline;
            this.reader = reader;
            this.contentInfo = contentInfo;
        }

        @Override
        public void run() {
            try {
                DefaultLogger.info(String.format("Starting pipeline. [name=%s]", pipeline.name()));
                DefaultLogger.trace(pipeline.name(), contentInfo);
                ReadResponse response = pipeline.read(reader, contentInfo);
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
