package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.state.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

@Getter
@Accessors(fluent = true)
public abstract class ProcessStateManager<E extends Enum<?>, T extends Offset> extends BaseStateManager {
    public static final String __ZK_PATH_SEQUENCE = "sequence";

    private final Class<? extends ProcessingState<E, T>> processingStateType;
    private ProcessingState<E, T> processingState;
    private String zkSequencePath;

    protected ProcessStateManager(@NonNull Class<? extends ProcessingState<E, T>> processingStateType) {
        this.processingStateType = processingStateType;
    }

    public void checkAgentState(@NonNull Class<? extends ProcessingState<E, T>> type) throws Exception {
        stateLock();
        try {
            CuratorFramework client = connection().client();

            if (client.checkExists().forPath(zkAgentStatePath()) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(zkAgentStatePath());
                if (Strings.isNullOrEmpty(path)) {
                    throw new StateManagerError(String.format("Error creating ZK base path. [path=%s]", basePath()));
                }
                processingState = processingStateType.getDeclaredConstructor().newInstance();
                processingState.setType(type.getCanonicalName());
                processingState.setNamespace(moduleInstance().getModule());
                processingState.setName(moduleInstance().getName());
                processingState.setTimeCreated(System.currentTimeMillis());
                processingState.setTimeUpdated(processingState().getTimeCreated());

                client.setData().forPath(zkAgentStatePath(),
                        JSONUtils.asBytes(processingState, processingState.getClass()));
            } else {
                processingState = readState(type);
            }
            processingState.setInstance(moduleInstance());
            processingState = update(processingState);
            zkSequencePath = new PathUtils.ZkPathBuilder(zkAgentPath())
                    .withPath(__ZK_PATH_SEQUENCE)
                    .build();
            if (client.checkExists().forPath(zkSequencePath) == null) {
                client.create().forPath(zkSequencePath);
                JSONUtils.write(client, zkSequencePath, new OffsetSequence());
            }
        } finally {
            stateUnlock();
        }
    }

    @Override
    protected void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                        @NonNull String path,
                        @NonNull BaseEnv<?> env,
                        @NonNull Class<? extends BaseStateManagerSettings> settingsType) throws Exception {
        super.init(xmlConfig, path, env, settingsType);
        checkAgentState(processingStateType);
    }

    public ProcessingState<E, T> initState(T txId) throws StateManagerError {
        checkState();
        Preconditions.checkNotNull(processingState);
        if (processingState.compareTx(txId) >= 0) return processingState;

        try {
            stateLock();
            try {
                processingState = readState(processingStateType);
                processingState.setProcessedOffset(txId);
                processingState.setTimeUpdated(System.currentTimeMillis());

                return update(processingState);
            } finally {
                stateUnlock();
            }
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public ProcessingState<E, T> update(@NonNull ProcessingState<E, T> state) throws Exception {
        checkState();
        ProcessingState<E, T> current = readState((Class<? extends ProcessingState<E, T>>) state.getClass());
        if (current.getTimeUpdated() > state.getTimeUpdated()) {
            throw new StateManagerError(String.format("Processing state is stale. [state=%s]", state));
        }
        state.setTimeUpdated(System.currentTimeMillis());
        state.setLastUpdatedBy(moduleInstance());
        CuratorFramework client = connection().client();
        JSONUtils.write(client, zkAgentStatePath(), processingState);
        return state;
    }

    public ProcessingState<E, T> update(@NonNull T value) throws Exception {
        processingState.setProcessedOffset(value);
        return update(processingState);
    }

    private ProcessingState<E, T> readState(Class<? extends ProcessingState<E, T>> type) throws StateManagerError {
        try {
            CuratorFramework client = connection().client();
            ProcessingState<E, T> state = JSONUtils.read(client, zkAgentStatePath(), type);
            if (state == null)
                throw new StateManagerError(String.format("NameNode State not found. [path=%s]", zkAgentStatePath()));
            return state;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public long nextSequence(int blockSize) throws StateManagerError {
        Preconditions.checkArgument(blockSize > 0);
        checkState();
        try {
            stateLock();
            try {
                CuratorFramework client = connection().client();
                OffsetSequence sequence = JSONUtils.read(client, zkSequencePath, OffsetSequence.class);
                if (sequence == null) {
                    throw new StateManagerError(String.format("Offset Sequence not found. [path=%s]", zkSequencePath));
                }
                long start = sequence.getSequence();
                sequence.setSequence(start + blockSize);
                JSONUtils.write(client, zkSequencePath, sequence);
                return start;
            } finally {
                stateUnlock();
            }
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }
}
