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

import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.mapping.model.InputContentInfo;
import io.zyient.base.core.mapping.pipeline.PipelineBuilder;
import io.zyient.base.core.mapping.pipeline.TransformerPipeline;
import io.zyient.base.core.mapping.readers.InputReader;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.stores.DataStoreManager;
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
    public static final String __CONFIG_PATH = "pipeline-executor";

    private final ProcessorState state = new ProcessorState();
    private PipelineBuilder builder;
    private MappingExecutorSettings settings;
    private ExecutorService executorService;

    public MappingExecutor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                @NonNull DataStoreManager dataStoreManager) throws ConfigurationException {
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
            return this;
        } catch (Exception ex) {
            state.error(ex);
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
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
                pipeline.read(reader, contentInfo);
                DefaultLogger.info(String.format("Finished pipeline. [name=%s]", pipeline.name()));
            } catch (Throwable ex) {
                String mesg = String.format("Pipeline failed: [error=%s][pipeline=%s]",
                        ex.getLocalizedMessage(), pipeline.name());
                DefaultLogger.stacktrace(ex);
                DefaultLogger.error(mesg);
            }
        }
    }
}
