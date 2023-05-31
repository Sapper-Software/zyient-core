package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.state.Offset;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;

@Getter
@Accessors(fluent = true)
public abstract class Processor<E extends Enum<?>, O extends Offset> implements Runnable, Closeable {
    protected final Logger LOG;
    protected final ProcessorState state = new ProcessorState();
    private String name;
    private final ProcessStateManager<E, O> stateManager;
    private ProcessingState<E, O> processingState;
    private final Class<? extends ProcessingState<E, O>> stateType;

    protected BaseEnv<?> env;
    @Getter(AccessLevel.PROTECTED)
    private DistributedLock __lock;
    @Getter(AccessLevel.NONE)
    private Thread executor;

    @SuppressWarnings("unchecked")
    protected Processor(@NonNull BaseEnv<?> env,
                        @NonNull Class<? extends ProcessingState<E, O>> stateType) {
        Preconditions.checkArgument(env.stateManager() instanceof ProcessStateManager);
        this.env = env;
        this.stateManager = (ProcessStateManager<E, O>) env.stateManager();
        this.stateType = stateType;
        this.LOG = LoggerFactory.getLogger(getClass());
    }

    protected void init(@NonNull ProcessorSettings settings) throws Exception {
        this.name = settings.getName();
        String lockPath = new PathUtils.ZkPathBuilder(String.format("processors/%s/%s/%s",
                env.moduleInstance().getModule(),
                env.moduleInstance().getName(),
                settings.getName()))
                .build();
        __lock = env.createLock(lockPath);
        __lock.lock();
        try {
            stateManager.checkAgentState(stateType);
            processingState = stateManager.processingState();
            processingState.clear();
            processingState = stateManager.update(processingState);
        } finally {
            __lock.unlock();
        }
    }

    public abstract Processor<E, O> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                         String path) throws ConfigurationException;

    public Processor<E, O> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        return init(xmlConfig, null);
    }


    @Override
    public void run() {
        Preconditions.checkState(state.isAvailable());
        Preconditions.checkNotNull(env);
        try {
            doRun();
        } catch (Throwable t) {
            state.error(t);
            DefaultLogger.error(LOG, "Message Processor terminated with error", t);
            DefaultLogger.stacktrace(t);
            try {
                updateError(t);
                BaseEnv.remove(env.name());
            } catch (Exception ex) {
                DefaultLogger.error(LOG, "Message Processor terminated with error", t);
                DefaultLogger.stacktrace(t);
            }
        }
    }

    protected abstract void doRun() throws Throwable;

    protected ProcessingState<E, O> updateState() throws Exception {
        processingState = stateManager.update(processingState);
        return processingState;
    }

    protected ProcessingState<E, O> updateState(@NonNull O offset) throws Exception {
        processingState.setOffset(offset);
        processingState = stateManager.update(processingState);
        return processingState;
    }

    protected ProcessingState<E, O> updateError(@NonNull Throwable t) throws Exception {
        processingState.error(t);
        return updateState();
    }

    public ProcessorState.EProcessorState stop() {
        Preconditions.checkNotNull(__lock);
        synchronized (state) {
            DefaultLogger.warn(LOG, String.format("[%s] Stopping processor...", name));
            if (state.isAvailable()) {
                state.setState(ProcessorState.EProcessorState.Stopped);
            }
        }
        __lock.lock();
        try {
            try {
                close();
                if (executor != null) {
                    executor.join();
                    DefaultLogger.info(LOG, String.format("[%s] Stopped executor thread...", name));
                }
            } catch (Exception ex) {
                DefaultLogger.stacktrace(ex);
                DefaultLogger.error(LOG, "Error stopping processor.", ex);
            }
            return state.getState();
        } finally {
            __lock.unlock();
            try {
                __lock.close();
                __lock = null;
            } catch (Exception ex) {
                DefaultLogger.stacktrace(ex);
                DefaultLogger.error(LOG, "Error disposing lock.", ex);
            }
        }
    }

    public void execute(@NonNull ExecutorService executor) throws Exception {
        synchronized (state) {
            if (state.isInitialized() || state.isAvailable()) {
                executor.submit(this);
            } else {
                throw new Exception(
                        String.format("[%s] Processor state is invalid: [state=%s]",
                                name, state.getState().name()));
            }
        }
    }

    public void start() throws Exception {
        synchronized (state) {
            if (state.isAvailable()) return;
            if (state.getState() != ProcessorState.EProcessorState.Initialized) {
                throw new Exception("Processor not initialized...");
            }
            state.setState(ProcessorState.EProcessorState.Running);
            executor = new Thread(this, String.format("PROCESSOR-[%s]", name));
            executor.start();
        }
    }

    public void pause() throws Exception {
        synchronized (state) {
            if (state.isRunning()) {
                state.setState(ProcessorState.EProcessorState.Paused);
            } else if (!state.isPaused()) {
                throw new Exception(
                        String.format("[%s] Invalid state: cannot pause. [state=%s]", name, state.getState().name()));
            }
        }
    }

    public void resume() throws Exception {
        synchronized (state) {
            if (state.isPaused()) {
                state.setState(ProcessorState.EProcessorState.Running);
            } else if (!state.isRunning()) {
                throw new Exception(
                        String.format("[%s] Invalid state: cannot resume. [state=%s]", name, state.getState().name()));
            }
        }
    }
}
