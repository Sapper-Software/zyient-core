package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.state.Offset;
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

@Getter
@Accessors(fluent = true)
public abstract class Processor<E extends Enum<?>, O extends Offset> implements Runnable, Closeable {
    protected final Logger LOG;
    protected final ProcessorState state = new ProcessorState();

    protected final ProcessStateManager<E, O> stateManager;
    protected BaseEnv<?> env;
    @Getter(AccessLevel.PACKAGE)
    private DistributedLock __lock;

    protected Processor(@NonNull ProcessStateManager<E, O> stateManager) {
        this.stateManager = stateManager;
        this.LOG = LoggerFactory.getLogger(getClass());
    }

    protected void init(@NonNull ProcessorSettings settings) throws Exception {
        String lockPath = new PathUtils.ZkPathBuilder(String.format("processors/%s/%s/%s",
                env.moduleInstance().getModule(),
                env.moduleInstance().getName(),
                settings.getName()))
                .build();
        __lock = env.createLock(lockPath);
        ProcessingState<E, O> s = stateManager.processingState();
        s.clear();
        stateManager.update(s);
    }

    public abstract Processor<E, O> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                         String path,
                                         @NonNull BaseEnv<?> env) throws ConfigurationException;

    public Processor<E, O> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                @NonNull BaseEnv<?> env) throws ConfigurationException {
        return init(xmlConfig, null, env);
    }

    public ProcessorState.EProcessorState stop() {
        try {
            if (state.isAvailable()) {
                state.setState(ProcessorState.EProcessorState.Stopped);
            }
            close();
            if (__lock != null) {
                __lock.close();
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(LOG, "Error stopping processor.", ex);
        }
        return state.getState();
    }
}
