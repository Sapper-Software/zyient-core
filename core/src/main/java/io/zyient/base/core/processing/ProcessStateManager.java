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

package io.zyient.base.core.processing;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.state.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class ProcessStateManager<E extends Enum<?>, T extends Offset> extends BaseStateManager {
    public static final String __ZK_PATH_SEQUENCE = "sequence";

    private final Class<? extends ProcessingState<E, T>> processingStateType;
    private final Map<String, ProcessingState<E, T>> processingStates = new HashMap<>();
    private String zkSequencePath;

    protected ProcessStateManager(@NonNull Class<? extends ProcessingState<E, T>> processingStateType) {
        this.processingStateType = processingStateType;
    }

    public ProcessingState<E, T> processingState(@NonNull String name) throws Exception {
        if (processingStates.containsKey(name)) {
            return processingStates.get(name);
        }
        throw new Exception(String.format("Processing state not found. [name=%s]", name));
    }

    public ProcessingState<E, T> checkAgentState(@NonNull String name) throws Exception {
        stateLock();
        try {
            return initState(name, null);
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
    }

    public ProcessingState<E, T> initState(@NonNull String name,
                                           T txId) throws Exception {
        CuratorFramework client = connection().client();
        String zp = new PathUtils.ZkPathBuilder(zkAgentStatePath())
                .withPath(name)
                .build();
        ProcessingState<E, T> processingState = null;
        if (client.checkExists().forPath(zp) == null) {
            String path = client.create().creatingParentContainersIfNeeded().forPath(zp);
            if (Strings.isNullOrEmpty(path)) {
                throw new StateManagerError(String.format("Error creating ZK base path. [path=%s]", basePath()));
            }
            processingState = processingStateType.getDeclaredConstructor().newInstance();
            processingState.setName(name);
            processingState.setType(processingStateType.getCanonicalName());
            processingState.setNamespace(moduleInstance().getModule());
            processingState.setName(moduleInstance().getName());
            processingState.setTimeCreated(System.currentTimeMillis());
            processingState.setTimeUpdated(processingState.getTimeCreated());
            processingState.setOffset(txId);
            client.setData().forPath(zp,
                    JSONUtils.asBytes(processingState));
        } else {
            processingState = readState(name);
            if (txId != null) {
                processingState.setOffset(txId);
                client.setData().forPath(zp,
                        JSONUtils.asBytes(processingState));
            }
        }
        processingState.setInstance(moduleInstance());
        processingState = update(name, processingState);
        zkSequencePath = new PathUtils.ZkPathBuilder(zkAgentPath())
                .withPath(__ZK_PATH_SEQUENCE)
                .build();
        if (client.checkExists().forPath(zkSequencePath) == null) {
            client.create().forPath(zkSequencePath);
            JSONUtils.write(client, zkSequencePath, new OffsetSequence());
        }
        processingStates.put(name, processingState);
        return processingState;
    }

    public ProcessingState<E, T> update(@NonNull String name,
                                        @NonNull ProcessingState<E, T> state) throws Exception {
        checkState();
        ProcessingState<E, T> current = readState(name);
        if (current.getTimeUpdated() > state.getTimeUpdated()) {
            throw new StateManagerError(String.format("Processing state is stale. [state=%s]", state));
        }
        state.setTimeUpdated(System.currentTimeMillis());
        state.setLastUpdatedBy(moduleInstance());
        CuratorFramework client = connection().client();
        String zp = new PathUtils.ZkPathBuilder(zkAgentStatePath())
                .withPath(name)
                .build();
        JSONUtils.write(client, zp, state);
        processingStates.put(name, state);
        return state;
    }

    public ProcessingState<E, T> update(@NonNull String name,
                                        @NonNull T value) throws Exception {
        ProcessingState<E, T> processingState = processingStates.get(name);
        if (processingState == null) {
            throw new Exception(String.format("Processing state not found. [name=%s]", name));
        }
        processingState.setOffset(value);
        return update(name, processingState);
    }

    private ProcessingState<E, T> readState(String name) throws StateManagerError {
        try {
            CuratorFramework client = connection().client();
            String zp = new PathUtils.ZkPathBuilder(zkAgentStatePath())
                    .withPath(name)
                    .build();
            ProcessingState<E, T> state = JSONUtils.read(client, zp, processingStateType);
            if (state == null)
                throw new StateManagerError(String.format("Processing State not found. [path=%s]", zp));
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
