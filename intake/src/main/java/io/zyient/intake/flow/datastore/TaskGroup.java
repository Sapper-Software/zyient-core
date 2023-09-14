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

package io.zyient.intake.flow.datastore;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.StateException;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.DataStoreException;
import io.zyient.base.core.stores.DataStoreManager;
import io.zyient.intake.flow.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import javax.annotation.Nonnull;
import java.security.Principal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ConfigPath(path = "taskGroup")
@Getter
@Setter
@Accessors(fluent = true)
public abstract class TaskGroup<K, T, C> {

    public static final String CONFIG_NODE_TASKS = "tasks";
    public static final String CONFIG_NODE_DATA_SOURCES = "dataSources";

    @Setter(AccessLevel.NONE)
    private String instanceId = UUID.randomUUID().toString();
    @Setter(AccessLevel.NONE)
    private TaskGroupState state = new TaskGroupState();
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private List<AbstractFlowTask<K, T>> tasks = new ArrayList<>();
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private TaskFlowErrorHandler<T> errorHandler;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private ReadWriteLock runLock = new ReentrantReadWriteLock();
    @Setter(AccessLevel.NONE)
    private final Class<? extends T> entityType;
    @Setter(AccessLevel.NONE)
    private final Class<? extends AbstractDataStore<C>> dataStoreType;
    @Setter(AccessLevel.NONE)
    private DataStoreManager dataStoreManager;
    @Setter(AccessLevel.NONE)
    private final Class<? extends TaskGroupSettings> settingsType;
    @Setter(AccessLevel.NONE)
    private TaskGroupSettings settings = null;
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private Map<String, ReentrantLock> storeRunLocks = new HashMap<>();
    private long lastRefreshed = 0;
    private BaseEnv<?> env;

    public TaskGroup(@Nonnull Class<? extends TaskGroupSettings> settingsType,
                     @Nonnull Class<? extends T> entityType,
                     @Nonnull Class<? extends AbstractDataStore<C>> dataStoreType) {
        this.entityType = entityType;
        this.dataStoreType = dataStoreType;
        this.settingsType = settingsType;
    }

    public TaskGroup<K, T, C> withInstanceId(@Nonnull String instanceId) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceId));
        this.instanceId = instanceId;

        return this;
    }

    public TaskGroup<K, T, C> withDataStoreManager(@Nonnull DataStoreManager dataStoreManager) {
        //this.dataStoreManager = dataStoreManager;
        return this;
    }

    public String name() {
        Preconditions.checkNotNull(settings);
        return settings().getName();
    }

    public String namespace() {
        Preconditions.checkNotNull(settings);
        return settings.getNamespace();
    }

    @SuppressWarnings("unchecked")
    public List<String> fetchDynamicStores() throws ConfigurationException {
        Preconditions.checkState(settings != null);
        Preconditions.checkState(settings.isDynamicStores());
        runLock.writeLock().lock();
        try {
            if ((System.currentTimeMillis() - lastRefreshed) < settings.getStoreRefreshInterval().normalized())
                return settings.getDataSourceNames();

            dataStoreManager.reLoadConfigurations();

            ZookeeperConnection connection = env.connectionManager()
                    .getConnection(settings.getZkConnection(), ZookeeperConnection.class);
            if (connection == null) {
                throw new ConfigurationException(
                        String.format("ZooKeeper Connection not found. [name=%s]", settings.getZkConnection()));
            }
            Preconditions.checkState(!Strings.isNullOrEmpty(settings.getZkPath()));
            CuratorFramework client = connection.client();
            String path = new PathUtils.ZkPathBuilder(settings.getZkPath())
                    .withPath(CONFIG_NODE_TASKS)
                    .withPath(settings.getNamespace())
                    .withPath(settings.getName())
                    .withPath(CONFIG_NODE_DATA_SOURCES)
                    .build();
            if (client.checkExists().forPath(path) != null) {
                Map<String, Boolean> current = new HashMap<>();
                for (String s : settings.getDataSourceNames()) {
                    current.put(s, true);
                }
                List<String> stores = client.getChildren().forPath(path);
                if (stores != null && !stores.isEmpty()) {
                    for (String store : stores) {
                        String cp = new PathUtils.ZkPathBuilder(path)
                                .withPath(store)
                                .build();
                        TaskGroupSource source = JSONUtils.read(client, cp, TaskGroupSource.class);
                        if (source != null) {
                            if (current.containsKey(source.getName()))
                                continue;
                            Class<? extends AbstractDataStore<?>> type
                                    = (Class<? extends AbstractDataStore<?>>) Class.forName(source.getType());
                            settings.getDataSourceNames().add(source.getName());
                            current.put(source.getName(), false);
                        }
                    }
                }
            }
            lastRefreshed = System.currentTimeMillis();
            for (String ds : settings.getDataSourceNames()) {
                if (!storeRunLocks.containsKey(ds)) {
                    storeRunLocks.put(ds, new ReentrantLock());
                }
            }
            return settings.getDataSourceNames();

        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        } finally {
            runLock.writeLock().unlock();
        }
    }

    public void configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                          @Nonnull BaseEnv<?> env) throws ConfigurationException {
        Lock __lock = runLock.writeLock();
        __lock.lock();
        try {
            this.env = env;
            ConfigReader reader = new ConfigReader(xmlConfig, settingsType);
            reader.read();
            settings = (TaskGroupSettings) reader.settings();

            instanceId = String.format("%s-%s", settings.getName(), instanceId);
            DefaultLogger.warn(String.format("Configuring Task Group. " + "[GROUP ID=%s][type=%s]",
                    instanceId, getClass().getCanonicalName()));

            if (settings.isDynamicStores()) {
                fetchDynamicStores();
            }
            if (settings.getDataSourceNames() == null || settings.getDataSourceNames().isEmpty()) {
                throw new ConfigurationException(
                        String.format("No data sources specified. [group=%s]", settings.getName()));
            }
            for (String store : settings.getDataSourceNames()) {
                DefaultLogger.info(String.format("Configured Data Store: [%s]", store));
                storeRunLocks.put(store, new ReentrantLock());
            }

            tasks = new ArrayList<>();
            configTasks(reader.config(), tasks);
            if (tasks.isEmpty()) {
                throw new ConfigurationException(String.format("No tasks defined. [group=%s]", settings.getName()));
            }
            configErrorHandler(reader.config());
            setup();

            state.setState(ETaskGroupState.Running);
            DefaultLogger.info(
                    String.format("Configured Task Group. " + "[GROUP ID=%s][type=%s][name=%s.%s]", instanceId,
                            getClass().getCanonicalName(), settings.getNamespace(), settings.getName()));
        } catch (Exception ex) {
            DefaultLogger.error(getClass().getCanonicalName(), ex);
            state.error(ex);
            throw new ConfigurationException(ex);
        } finally {
            __lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private void configErrorHandler(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig)
            throws ConfigurationException {
        if (settings.getErrorHandlerSettingsType() == null)
            return;
        String node = ConfigReader.getPathAnnotation(settings.getErrorHandlerSettingsType());
        Preconditions.checkArgument(!Strings.isNullOrEmpty(node));
        if (!ConfigReader.checkIfNodeExists(xmlConfig, node)) {
            return;
        }
        ConfigReader reader = new ConfigReader(xmlConfig, node, settings.getErrorHandlerSettingsType());
        reader.read();
        TaskFlowErrorHandlerSettings errorSettings = (TaskFlowErrorHandlerSettings) reader.settings();
        try {
            errorHandler = (TaskFlowErrorHandler<T>) ReflectionUtils.createInstance(errorSettings.getType());
            errorHandler.configure(reader.config(), env);
        } catch (Exception e) {
            DefaultLogger.error(getClass().getCanonicalName(), e);
            throw new ConfigurationException(e);
        }
    }

    public void configTasks(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                            @Nonnull List<AbstractFlowTask<K, T>> tasks) throws ConfigurationException {
        HierarchicalConfiguration<ImmutableNode> tsnode = xmlConfig.configurationAt(CONFIG_NODE_TASKS);
        if (tsnode != null) {
            String tname = ConfigReader.getPathAnnotation(AbstractFlowTask.class);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(tname));
            List<HierarchicalConfiguration<ImmutableNode>> nodes = xmlConfig.configurationsAt(tname);
            if (!nodes.isEmpty()) {
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    configTask(node, tasks);
                }
            } else {
                throw new ConfigurationException(String.format("Invalid Configuration " + "node. [path=%s][type=%s]",
                        tsnode, tsnode.getClass().getCanonicalName()));
            }

        }
    }

    @SuppressWarnings("unchecked")
    private void configTask(HierarchicalConfiguration<ImmutableNode> node,
                            List<AbstractFlowTask<K, T>> tasks) throws ConfigurationException {
        ConfigReader reader = new ConfigReader(node, null, FlowTaskSettings.class);
        reader.read();
        FlowTaskSettings taskSettings = (FlowTaskSettings) reader.settings();
        try {
            Class<? extends AbstractFlowTask<K, T>> tc
                    = (Class<? extends AbstractFlowTask<K, T>>) taskSettings.getType();
            AbstractFlowTask<K, T> task = ReflectionUtils.createInstance(tc);
            task.withTaskGroup(this).withSettings(taskSettings).configure(node, env);
            tasks.add(task);

            DefaultLogger.info(String.format("[%s][%s] Added task. [name=%s][type=%s]",
                    settings.getNamespace(), settings.getName(),
                    task.settings().getName(), task.getClass().getCanonicalName()));
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException(e);
        } catch (Exception e) {
            DefaultLogger.error(getClass().getCanonicalName(), e);
            throw new ConfigurationException(e);
        }
    }

    public TaskGroupResponse<T> run(@Nonnull TaskContext context, @Nonnull Principal user) throws FlowTaskException {
        // Preconditions.checkArgument(dataSourceNames.size() == 1);
        try {
            state.checkState(getClass(), ETaskGroupState.Running);
            AbstractDataStore<C> dataSource = getDataSource(null);
            if (dataSource == null) {
                throw new FlowTaskException("Default Data Source not registered.");
            }
            try {
                return run(context, dataSource, user);
            } finally {
                dataSource.close();
            }
        } catch (Throwable ex) {
            DefaultLogger.error(getClass().getCanonicalName(), ex);
            throw new FlowTaskException(ex);
        }
    }

    public Map<String, TaskGroupResponse<T>> runAll(@Nonnull TaskContext context, @Nonnull Principal user)
            throws FlowTaskException {
        try {
            state.checkState(getClass(), ETaskGroupState.Running);
            if (settings().isDynamicStores()) {
                if ((System.currentTimeMillis() - lastRefreshed) > settings.getStoreRefreshInterval().normalized()) {
                    fetchDynamicStores();
                }
            }
            Preconditions.checkState(settings.getDataSourceNames() != null
                    && !settings.getDataSourceNames().isEmpty());

            Map<String, TaskGroupResponse<T>> results = new ConcurrentHashMap<>();

            settings.getDataSourceNames().parallelStream().forEach(store -> {
                TaskGroupResponse<T> result = null;
                try {
                    DefaultLogger.info(String.format(
                            "Dynamic TG Running Data Sources in Parallel taskGroup [%s.%s] for data source [%s]...",
                            settings.getNamespace(), settings.getName(), store));
                    result = run(new TaskContext(), store, user);
                    DefaultLogger.trace(result);
                    results.put(store, result);
                } catch (Exception e) {
                    DefaultLogger.error(getClass().getCanonicalName(), e);
                    if (result != null) {
                        results.put(store, result);
                    }
                }
            });

//			for (String store : dataSourceNames) {
//				TaskGroupResponse<T> result = null;
//				try {
//					LogUtils.info(getClass(),
//							String.format("Running taskGroup [%s.%s] for data source [%s]...", namespace, name, store));
//					result = run(context, store, user);
//					LogUtils.debug(getClass(), result);
//					results.put(store, result);
//				} catch (Exception e) {
//					LogUtils.error(getClass(), e);
//					if (result != null) {
//						results.put(store, result);
//					}
//				}
//			}
            return results;
        } catch (Throwable ex) {
            DefaultLogger.error(getClass().getCanonicalName(), ex);
            throw new FlowTaskException(ex);
        }
    }

    public TaskGroupResponse<T> run(@Nonnull TaskContext context, @Nonnull String dataSourceName,
                                    @Nonnull Principal user) throws FlowTaskException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dataSourceName));
        try {
            state.checkState(getClass(), ETaskGroupState.Running);
            AbstractDataStore<C> dataSource = getDataSource(dataSourceName);
            if (dataSource == null) {
                throw new FlowTaskException(
                        String.format("Specified Data Source not registered. " + "[name=%s]", dataSourceName));
            }
            try {
                return run(context, dataSource, user);
            } finally {
                dataSource.close();
            }
        } catch (Throwable ex) {
            DefaultLogger.error(getClass().getCanonicalName(), ex);
            throw new FlowTaskException(ex);
        }
    }

    private AbstractDataStore<C> getDataSource(String name) throws DataStoreException, StateException {
        Preconditions.checkState(dataStoreManager() != null);
        if (Strings.isNullOrEmpty(name)) {
            if (settings.getDataSourceNames().size() > 1) {
                throw new DataStoreException(
                        String.format("[%s][%s] Multiple data sources loaded. Please specify the data source name.",
                                settings.getNamespace(), settings.getName()));
            }
            String ds = settings.getDataSourceNames().get(0);
            return dataStoreManager().getDataStore(ds, dataStoreType);
        } else {
            return dataStoreManager().getDataStore(name, dataStoreType);
        }
    }

    public TaskGroupResponse<T> run(@Nonnull TaskContext context,
                                    @Nonnull AbstractDataStore<C> dataSource,
                                    @Nonnull Principal user) throws FlowTaskException {
        if (settings.isAllowConcurrentPerStore()) {
            return doRun(context, dataSource, user);
        } else {
            ReentrantLock lock = storeRunLocks.get(dataSource.name());
            if (lock == null) {
                throw new FlowTaskException(
                        String.format("Lock not registered for data store. [name=%s]", dataSource.name()));
            }
            if (lock.tryLock()) {
                try {
                    return doRun(context, dataSource, user);
                } finally {
                    lock.unlock();
                }
            } else {
                DefaultLogger.warn(String.format("DataStore being processed. [name=%s]", dataSource.name()));
                return null;
            }
        }
    }

    private TaskGroupResponse<T> doRun(@Nonnull TaskContext context,
                                       @Nonnull AbstractDataStore<C> dataSource,
                                       @Nonnull Principal user) throws FlowTaskException {
        TaskGroupResponse<T> response = new TaskGroupResponse<>();
        response.setNamespace(settings.getNamespace());
        response.setName(settings().getName());
        response.setInstanceId(instanceId);
        response.setRunId(UUID.randomUUID().toString());
        response.setDataSource(dataSource.name());
        response.setStartTime(System.currentTimeMillis());
        try {
            state.checkState(getClass(), ETaskGroupState.Running);
            context = preRunSetup(context, dataSource);
            context.setTaskId(instanceId);
            context.setRunId(response.getRunId());
            context.setTaskStartTime(System.currentTimeMillis());

            Lock __lock = runLock.readLock();
            Collection<T> data = null;
            __lock.lock();

            try {
                DefaultLogger.debug(String.format("Running Task Group. [name=%s]", settings.getName()));
                // LogUtils.debug(getClass(), context);
                List<TaskExecutionRecord<T>> responses = new ArrayList<>();
                TaskAuditRecord taskAuditRecord = null;
                if (settings.isAudited()) {
                    taskAuditRecord = TaskAuditManager.get().create(this, context, dataSource.name(),
                            getStartAuditParams(context));
                }
                try {
                    data = fetch(context, dataSource);

                    if (data != null && !data.isEmpty()) {
                        response.setResults(responses);
                        response.setRecordCount(data.size());
                        DefaultLogger.debug(String.format("Fetched data records. [count=%d]", data.size()));
                        for (T record : data) {
                            runTasks(context, dataSource, record, taskAuditRecord, response, user);
                        }
                        if (taskAuditRecord != null) {
                            TaskAuditManager.get().finish(context, getFinishAuditParams(context));
                        }
                    } else {
                        String mesg = String.format("[%s][%s] No data fetched for " + "current run...",
                                settings.getNamespace(),
                                settings.getName());
                        DefaultLogger.warn(mesg);
                        if (taskAuditRecord != null)
                            TaskAuditManager.get().error(taskAuditRecord.getTaskId().getTaskId(),
                                    new FlowTaskException(mesg));
                    }
                } catch (Throwable e) {
                    if (taskAuditRecord != null) {
                        TaskAuditManager.get().error(taskAuditRecord.getTaskId().getTaskId(), e);
                        throw e;
                    }
                } finally {
                    DefaultLogger.debug("Calling post run handler...");
                    postRunHandler(context, data, responses, dataSource);
                    dataStoreManager().closeStores();
                }
            } finally {
                response.setEndTime(System.currentTimeMillis());
                context.setTaskEndTime(System.currentTimeMillis());
                __lock.unlock();
            }
        } catch (Throwable ex) {
            DefaultLogger.error(getClass().getCanonicalName(), ex);
            response.setError(ex);
        }
        DefaultLogger.trace(response);
        return response;
    }

    public void runTasks(TaskContext context,
                         AbstractDataStore<C> dataSource,
                         T record,
                         TaskAuditRecord taskAuditRecord,
                         TaskGroupResponse<T> result,
                         Principal user) throws Throwable {
        TaskContext localContext = setupRunContext(context, dataSource);
        TaskExecutionRecord<T> executionRecord = new TaskExecutionRecord<>();
        executionRecord.setStartTime(System.currentTimeMillis());
        executionRecord.setContext(localContext);

        for (AbstractFlowTask<K, T> task : tasks) {
            TaskResponse response = null;
            try {
                if (taskAuditRecord != null) {
                    TaskAuditManager.get().step(context, task.name());
                }
                response = runTask(task, record, localContext, user);
                if (response.getState() == ETaskResponse.Stop) {
                    DefaultLogger.warn(
                            String.format("Stopped processing for record. [run ID=%s]", context.getRunId()));
                    break;
                } else if (response.getState() == ETaskResponse.StopWithError
                        || response.getState() == ETaskResponse.MoveToError) {
                    Throwable ex = response.getNonFatalError();
                    if (ex == null) {
                        ex = response.getError();
                    }
                    if (ex != null)
                        DefaultLogger.error(getClass().getCanonicalName(), ex);
                    result.setErrorCount(result.getErrorCount() + 1);
                    break;
                } else if (response.getState() == ETaskResponse.Error) {
                    throw new FlowTaskException(response.getError());
                }
                dataStoreManager().commit();
            } catch (Throwable e) {
                dataStoreManager().rollback();
                DefaultLogger.error(getClass().getCanonicalName(), e);
                throw e;
            } finally {
                executionRecord.setEndTime(System.currentTimeMillis());
                executionRecord.addResponse(task.name(), response);
                result.getResults().add(executionRecord);
            }
        }
    }

    public TaskResponse runTask(AbstractFlowTask<K, T> task, T data, TaskContext context, Principal user)
            throws Throwable {
        TaskResponse response = task.run(data, context, user);
        if (response.getState() == ETaskResponse.Error) {
            DefaultLogger.error(getClass().getCanonicalName(), response.getError());
            if (errorHandler != null) {
                errorHandler.handleError(context, response, data, user);
            }
            throw response.getError();
        } else if (response.getState() == ETaskResponse.ContinueWithError) {
            DefaultLogger.error(
                    String.format("[GROUP ID=%s][name=%s" + "][task=%s] " + "Continue with error --> %s", instanceId,
                            settings.getName(), task.name(), response.getNonFatalError().getLocalizedMessage()));
            DefaultLogger.error(getClass().getCanonicalName(), response.getNonFatalError());
        } else if (response.getState() == ETaskResponse.StopWithError
                || response.getState() == ETaskResponse.MoveToError) {
            DefaultLogger.error(
                    String.format("[GROUP ID=%s][name=%s" + "][task=%s] " + "Stopping with error --> %s", instanceId,
                            settings.getName(), task.name(), response.getNonFatalError().getLocalizedMessage()));
            DefaultLogger.error(getClass().getCanonicalName(), response.getNonFatalError());
            if (errorHandler != null) {
                errorHandler.handleError(context, response, data, user);
            }
        } else if (response.getState() == ETaskResponse.Stop) {
            DefaultLogger.info(
                    String.format("[GROUP ID=%s][name=%s" + "][task=%s] " + "Stopping execution chain.", instanceId,
                            settings.getName(), task.name()));
        } else {
            DefaultLogger.debug(
                    String.format("[GROUP ID=%s][name=%s" + "][task=%s] " + "Task completed successfully.", instanceId,
                            settings.getName(), task.name()));
        }
        return response;
    }

    public void dispose() {
        DefaultLogger.warn(String.format("Disposing Task Group. [GROUP ID=%s][name=%s]", instanceId, settings.getName()));
        Lock __lock = runLock.writeLock();
        __lock.lock();
        try {
            if (state.getState() == ETaskGroupState.Running) {
                state.setState(ETaskGroupState.Stopped);
                DefaultLogger.warn(
                        String.format("Disposed Task Group. [GROUP ID=%s][name=%s]", instanceId, settings.getName()));
            } else {
                DefaultLogger.warn(String.format("Task Group not running. " + "[GROUP ID=%s][name=%s][state=%s]",
                        instanceId, settings.getName(), state.getState().name()));
            }
        } finally {
            __lock.unlock();
        }
    }

    public int recover() throws FlowTaskException {
        try {
            int count = 0;
            for (String dataSourceName : settings.getDataSourceNames()) {
                AbstractDataStore<C> dataStore = getDataSource(dataSourceName);
                List<T> data = recover(dataStore);
                if (data != null) {
                    count += data.size();
                }
            }
            return count;
        } catch (Exception ex) {
            throw new FlowTaskException(ex);
        }
    }

    protected Map<String, String> getStartAuditParams(TaskContext context) {
        return null;
    }

    protected Map<String, String> getFinishAuditParams(TaskContext context) {
        return null;
    }

    /**
     * Setup delegate to customize instance.
     *
     * @throws ConfigurationException
     */
    protected abstract void setup() throws ConfigurationException;

    /**
     * Fetch the next set of data records to be processed.
     *
     * @param context    - Run context handle.
     * @param dataSource - Data Source instance to fetch data from.
     * @return - List of fetched records. (NULL if not records to process)
     * @throws FlowTaskException
     */
    protected abstract Collection<T> fetch(@Nonnull TaskContext context, @Nonnull AbstractDataStore<C> dataSource)
            throws FlowTaskException;

    /**
     * Recover any pending processing messages.
     *
     * @param dataSource - Data Source instance to fetch data from.
     * @return - List of fetched records. (NULL if not records to process)
     * @throws FlowTaskException
     */
    protected abstract List<T> recover(@Nonnull AbstractDataStore<C> dataSource) throws FlowTaskException;

    /**
     * Delegate method to be called before every run batch.
     *
     * @param context   - Task Context.
     * @param dataStore - Data Store handle.
     * @throws FlowTaskException
     */
    protected abstract TaskContext preRunSetup(@Nonnull TaskContext context, @Nonnull AbstractDataStore<C> dataStore)
            throws FlowTaskException;

    /**
     * Setup the run context for this run cycle.
     *
     * @param context    - Source context handle.
     * @param dataSource - Data Source instance to fetch data from.
     * @return - Run context handle.
     * @throws FlowTaskException
     */
    protected abstract TaskContext setupRunContext(@Nonnull TaskContext context,
                                                   @Nonnull AbstractDataStore<C> dataSource) throws FlowTaskException;

    /**
     * Delegate method is called at the end of each run. The base method doesn't do
     * anything.
     *
     * @param data       - List of data records fetched for this run.
     * @param records    - Execution response list.
     * @param dataSource - Data Source instance to fetch data from.
     * @throws FlowTaskException
     */
    protected abstract void postRunHandler(@Nonnull TaskContext taskContext, Collection<T> data,
                                           List<TaskExecutionRecord<T>> records, @Nonnull AbstractDataStore<C> dataSource) throws FlowTaskException;
}
