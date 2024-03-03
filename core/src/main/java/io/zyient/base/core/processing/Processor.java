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
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.DistributedLock;
import io.zyient.base.core.state.Offset;
import io.zyient.base.core.utils.MetricsBase;
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
    private ProcessStateManager<E, O> stateManager;
    private final Class<? extends ProcessingState<E, O>> stateType;
    protected BaseEnv<?> env;
    protected MetricsBase metrics;

    private String name;
    @Getter(AccessLevel.PROTECTED)
    private DistributedLock __lock;
    @Getter(AccessLevel.NONE)
    private Thread executor;
    private ProcessorSettings settings;
    private ProcessingState<E, O> processingState;

    @SuppressWarnings("unchecked")
    protected Processor(@NonNull Class<? extends ProcessingState<E, O>> stateType) {
        this.stateType = stateType;
        this.LOG = LoggerFactory.getLogger(getClass());
    }

    @SuppressWarnings("unchecked")
    protected void init(@NonNull ProcessorSettings settings,
                        @NonNull BaseEnv<?> env) throws Exception {
        this.settings = settings;
        this.name = settings.getName();
        this.env = env;
        Preconditions.checkArgument(env.stateManager() instanceof ProcessStateManager);
        this.stateManager = (ProcessStateManager<E, O>) env.stateManager();
        String lockPath = new PathUtils.ZkPathBuilder(String.format("processors/%s/%s/%s",
                env.moduleInstance().getModule(),
                env.moduleInstance().getName(),
                name))
                .build();
        __lock = env.createLock(lockPath);
        __lock.lock();
        try {
            processingState = stateManager.checkAgentState(name);
            initState(processingState);
            processingState = stateManager.update(name, processingState);
        } finally {
            __lock.unlock();
        }
    }

    protected Processor<E, O> withMetrics(@NonNull MetricsBase metrics) {
        this.metrics = metrics;
        return this;
    }

    public abstract Processor<E, O> init(@NonNull BaseEnv<?> env,
                                         @NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                         String path) throws ConfigurationException;

    public Processor<E, O> init(@NonNull BaseEnv<?> env,
                                @NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        return init(env, xmlConfig, null);
    }

    protected abstract void initState(@NonNull ProcessingState<E, O> processingState) throws Exception;

    @Override
    public void run() {
        Preconditions.checkState(state.isAvailable());
        Preconditions.checkNotNull(env);
        try {
            doRun(false);
            processingState = finished(processingState);
            processingState = stateManager.update(name, processingState);
            close();
        } catch (Throwable t) {
            DefaultLogger.error(LOG, "Message Processor terminated with error", t);
            DefaultLogger.stacktrace(t);
            try {
                updateError(t);
                BaseEnv.remove(env.name());
            } catch (Exception ex) {
                DefaultLogger.error(LOG, "Message Processor terminated with error", t);
                DefaultLogger.stacktrace(t);
            } finally {
                state.error(t);
            }
        }
    }

    public void runOnce() throws Throwable {
        Preconditions.checkState(state.isAvailable());
        Preconditions.checkNotNull(env);
        try {
            __lock.lock();
            try {
                doRun(true);
                processingState = finished(processingState);
                processingState = stateManager.update(name, processingState);
            } finally {
                __lock.unlock();
            }
        } catch (Throwable t) {
            DefaultLogger.error(LOG, "Message Processor terminated with error", t);
            DefaultLogger.stacktrace(t);
            updateError(t);
            state.error(t);
            throw t;
        }
    }


    protected abstract ProcessingState<E, O> finished(@NonNull ProcessingState<E, O> processingState);

    protected abstract void doRun(boolean runOnce) throws Throwable;

    protected ProcessingState<E, O> updateState() throws Exception {
        processingState = stateManager.update(name, processingState);
        return processingState;
    }

    protected ProcessingState<E, O> updateState(@NonNull O offset) throws Exception {
        processingState.setOffset(offset);
        processingState = stateManager.update(name, processingState);
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
