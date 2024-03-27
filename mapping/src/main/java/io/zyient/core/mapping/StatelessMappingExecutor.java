package io.zyient.core.mapping;

import com.google.common.base.Preconditions;
import io.zyient.base.common.StateException;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.ConnectionManager;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.model.SourceInputContentInfo;
import io.zyient.core.mapping.pipeline.Pipeline;
import io.zyient.core.mapping.pipeline.PipelineBuilder;
import io.zyient.core.mapping.pipeline.PipelineHandle;
import io.zyient.core.mapping.pipeline.PipelineInMemory;
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
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Getter
@Accessors(fluent = true)
public class StatelessMappingExecutor implements Closeable {
    public static final String __CONFIG_PATH_PIPELINE = "executor";
    public static final String __CONFIG_PATH_EXECUTOR = "settings";

    private final ProcessorState state = new ProcessorState();
    private PipelineBuilder builder;
    private MappingExecutorSettings settings;
    private ExecutorService executorService;
    private DataStoreManager dataStoreManager;
    private ConnectionManager connectionManager;
    private DataStoreEnv<?> env;

    public StatelessMappingExecutor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
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
        StatelessMappingExecutor.Reader reader = new StatelessMappingExecutor.Reader(handle.pipeline(),
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
    private static StatelessMappingExecutor __instance;

    public static StatelessMappingExecutor defaultInstance() throws Exception {
        Preconditions.checkNotNull(__instance);
        __instance.checkState();
        return __instance;
    }

    public static StatelessMappingExecutor create(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                  @NonNull BaseEnv<?> env) throws Exception {
        __instance = new StatelessMappingExecutor();
        return __instance.init(xmlConfig, env);
    }

    private record Reader(Pipeline pipeline,
                          InputContentInfo contentInfo) implements Runnable {

        @Override
        public void run() {
            try {
                if (!(pipeline instanceof PipelineInMemory)) {
                    throw new Exception(String.format("Invalid pipeline, not a in memory pipeline. [name=%s][type=%s]",
                            pipeline.name(), pipeline.getClass().getCanonicalName()));
                }
                DefaultLogger.info(String.format("Starting pipeline. [name=%s]", pipeline.name()));
                DefaultLogger.trace(pipeline.name(), contentInfo);
                Preconditions.checkArgument(contentInfo instanceof SourceInputContentInfo);
                ReadResponse response = ((PipelineInMemory) pipeline).read((SourceInputContentInfo) contentInfo);
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
