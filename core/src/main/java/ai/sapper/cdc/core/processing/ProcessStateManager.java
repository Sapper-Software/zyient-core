package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.core.state.BaseStateManager;
import ai.sapper.cdc.core.state.Offset;
import ai.sapper.cdc.core.state.OffsetState;
import ai.sapper.cdc.core.state.StateManagerError;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.curator.framework.CuratorFramework;

@Getter
@Accessors(fluent = true)
public abstract class ProcessStateManager<E extends Enum<?>, T extends Offset> extends BaseStateManager {
    private final Class<? extends ProcessingState<E, T>> processingStateType;
    private ProcessingState<E, T> processingState;

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
                processingState.setNamespace(moduleInstance().getModule());
                processingState.setUpdatedTime(System.currentTimeMillis());

                client.setData().forPath(zkAgentStatePath(),
                        JSONUtils.asBytes(processingState, processingState.getClass()));
            } else {
                processingState = readState(type);
            }
            processingState.setInstance(moduleInstance());
            processingState = update(processingState);
        } finally {
            stateUnlock();
        }
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
                processingState.setUpdatedTime(System.currentTimeMillis());

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
        ProcessingState<E, T> current = readState((Class<? extends ProcessingState<E, T>>) state.getClass());
        if (current.getUpdatedTime() > state.getUpdatedTime()) {
            throw new StateManagerError(String.format("Processing state is stale. [state=%s]", state));
        }
        state.setUpdatedTime(System.currentTimeMillis());
        CuratorFramework client = connection().client();
        client.setData().forPath(zkAgentStatePath(),
                JSONUtils.asBytes(processingState, processingState.getClass()));
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
}
