package ai.sapper.cdc.core.io;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.processing.ProcessorState;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public abstract class FileSystemHelper implements Runnable {
    public static final Logger LOG = LoggerFactory.getLogger(FileSystemHelper.class);

    private final ProcessorState state = new ProcessorState();
    private final String fsRoot;
    private long fileRetentionTime = -1;
    private long cleanupInterval = 5 * 60 * 1000;
    private final FileSystem fs;
    private FileSystemHelperConfig config;
    private Thread runner;

    public FileSystemHelper(@NonNull FileSystem fs,
                            @NonNull String fsRoot) {
        this.fs = fs;
        this.fsRoot = fsRoot;
    }

    public FileSystemHelper init(@NonNull HierarchicalConfiguration<ImmutableNode> xml,
                                 @NonNull Class<? extends FileSystemHelperConfig> configType)
            throws IOException {
        try {
            config = configType.getConstructor(HierarchicalConfiguration.class)
                    .newInstance(xml);
            config.read(configType);
            if (config.retentionTime > 0) {
                fileRetentionTime = config.retentionTime;
            }
            if (config.cleanupInterval > 0) {
                cleanupInterval = config().cleanupInterval;
            }
            state.state(ProcessorState.EProcessorState.Initialized);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new IOException(ex);
        }
    }

    public void start() throws IOException {
        Preconditions.checkState(state.isInitialized());
        synchronized (state) {
            if (state.isRunning()) {
                return;
            }
            state.state(ProcessorState.EProcessorState.Running);
            onStart();
            runner = new Thread(this);
            runner.start();
        }
    }

    public void stop() {
        synchronized (state) {
            if (state.isRunning()) {
                state.state(ProcessorState.EProcessorState.Stopped);
            }
        }
    }

    @Override
    public void run() {
        try {
            DefaultLogger.warn(LOG, "File System Helper started...");
            while (true) {
                Thread.sleep(cleanupInterval);
                if (!state.isRunning()) {
                    break;
                }
                doRun();
            }
            onStop();
            DefaultLogger.warn(LOG, "File System Helper stopped...");
        } catch (Throwable t) {
            DefaultLogger.error(LOG, String.format("File System Helper Stopped: [error=%s]", t.getLocalizedMessage()));
            DefaultLogger.stacktrace(t);
            state.error(t);
        }
    }


    public abstract void doRun() throws IOException;

    public abstract void onStart() throws IOException;

    public abstract void onStop() throws IOException;

    @Getter
    @Accessors(fluent = true)
    public static class FileSystemHelperConfig extends ConfigReader {
        public static final String __CONFIG_PATH = "helper";
        @Config(name = "retention", required = false)
        private long retentionTime = -1;
        @Config(name = "frequency", required = false)
        private long cleanupInterval = -1;

        public FileSystemHelperConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }
    }
}
