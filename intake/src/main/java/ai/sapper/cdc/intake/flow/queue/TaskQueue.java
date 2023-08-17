package ai.sapper.cdc.intake.flow.queue;

import com.codekutter.common.messaging.AbstractQueue;
import com.codekutter.common.messaging.QueueManager;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigListNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.transformers.StringListParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.ingestion.common.flow.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ConfigPath(path = "taskGroup")
@Getter
@Setter
@Accessors(fluent = true)
public abstract class TaskQueue<K, T, C, M> implements IConfigurable {
    public static final String CONFIG_NODE_TASKS = "tasks";
    public static final String CONFIG_NODE_DATA_SOURCES = "queue";

    @ConfigAttribute(name = "name", required = true)
    private String name;
    @ConfigAttribute(name = "namespace", required = true)
    private String namespace;
    @ConfigValue(name = "dataSources", parser = StringListParser.class)
    private List<String> queueNames;

    @Setter(AccessLevel.NONE)
    private String taskId;
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
    private Class<? extends T> entityType;
    @Setter(AccessLevel.NONE)
    private Class<? extends C> connectionType;
    @Setter(AccessLevel.NONE)
    private Class<? extends M> messageType;

    public TaskQueue(@Nonnull Class<? extends T> entityType,
                     @Nonnull Class<? extends C> connectionType,
                     @Nonnull Class<? extends M> messageType) {
        this.entityType = entityType;
        this.connectionType = connectionType;
        this.messageType = messageType;
    }

    public TaskQueue<K, T, C, M> withTaskId(@Nonnull String taskId) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(taskId));
        this.taskId = taskId;

        return this;
    }

    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        LogUtils.warn(getClass(), String.format("Configuring Task Group. " +
                "[RUNID=%s][type=%s][path=%s]", taskId, getClass().getCanonicalName(), node.getAbsolutePath()));
        Lock __lock = runLock.writeLock();
        __lock.lock();
        try {
            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            if (Strings.isNullOrEmpty(name)) {
                throw ConfigurationException.propertyNotFoundException("name");
            }
            AbstractConfigNode tnode = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
            if (!(tnode instanceof ConfigPathNode)) {
                throw new ConfigurationException(String.format("Configuration node not found. " +
                                "[node=%s][path=%s]", ConfigUtils.getAnnotationPath(getClass()),
                        node.getAbsolutePath()));
            }

            configTasks((ConfigPathNode) tnode);
            configErrorHandler((ConfigPathNode) tnode);
            setup(tnode);

            state.setState(ETaskGroupState.Running);
            LogUtils.warn(getClass(), String.format("Configured Task Group. " +
                            "[RUNID=%s][type=%s][path=%s][name=%s]", taskId, getClass().getCanonicalName(),
                    node.getAbsolutePath(), name));
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            state.setError(ex);
            throw new ConfigurationException(ex);
        } finally {
            __lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private void configErrorHandler(ConfigPathNode node) throws ConfigurationException {
        AbstractConfigNode dnode = ConfigUtils.getPathNode(TaskFlowErrorHandler.class, node);
        if (dnode == null) {
            return;
        }
        String cls = ConfigUtils.getClassAttribute(dnode);
        if (Strings.isNullOrEmpty(cls)) {
            throw new ConfigurationException(String.format("Error handler class not found. " +
                    "[path=%s]", node.getAbsolutePath()));
        }
        try {
            Class<?> tc = Class.forName(cls);
            Object obj = tc.newInstance();
            if (obj instanceof TaskFlowErrorHandler) {
                errorHandler = (TaskFlowErrorHandler<T>) obj;
                errorHandler.withTaskGroup(this).configure(dnode);
            } else {
                throw new ConfigurationException(String.format("Invalid Error Handler class " +
                        "specified. " +
                        "[type=%s]", tc.getCanonicalName()));
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException(e);
        }
    }

    private void configTasks(ConfigPathNode node) throws ConfigurationException {
        AbstractConfigNode tsnode = node.find(CONFIG_NODE_TASKS);
        if (tsnode != null) {
            String tname = ConfigUtils.getAnnotationPath(AbstractFlowTask.class);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(tname));

            tasks = new ArrayList<>();
            if (tsnode instanceof ConfigPathNode) {
                AbstractConfigNode tnode = ConfigUtils.getPathNode(AbstractFlowTask.class,
                        (ConfigPathNode) tsnode);
                if (tnode != null)
                    configTask((ConfigPathNode) tnode);
            } else if (tsnode instanceof ConfigListNode) {
                ConfigListNode<?> clnode = (ConfigListNode<?>) tsnode;
                for (Object obj : clnode.getValues()) {
                    if (obj instanceof ConfigPathNode) {
                        configTask((ConfigPathNode) obj);
                    } else {
                        throw new ConfigurationException(String.format("Invalid Configuration " +
                                        "node. [path=%s][type=%s]",
                                ((AbstractConfigNode) obj).getAbsolutePath(),
                                obj.getClass().getCanonicalName()));
                    }
                }
            } else {
                throw new ConfigurationException(String.format("Invalid Configuration " +
                                "node. [path=%s][type=%s]",
                        tsnode,
                        tsnode.getClass().getCanonicalName()));
            }

        }
    }

    @SuppressWarnings("unchecked")
    private void configTask(ConfigPathNode node) throws ConfigurationException {
        String nname = ConfigUtils.getAnnotationPath(AbstractFlowTask.class);
        Preconditions.checkArgument(node.getName().compareTo(nname) == 0);
        String cls = ConfigUtils.getClassAttribute(node);
        if (Strings.isNullOrEmpty(cls)) {
            throw new ConfigurationException(String.format("Task class not found. " +
                    "[path=%s]", node.getAbsolutePath()));
        }
        try {
            Class<?> tc = Class.forName(cls);
            Object obj = tc.newInstance();
            if (obj instanceof AbstractFlowTask) {
                AbstractFlowTask<K, T> task = (AbstractFlowTask<K, T>) obj;
                String name = ConfigUtils.getNameAttribute(node);
                task.name(name);
                task.withTaskGroup(this).configure(node);

                tasks.add(task);

                LogUtils.info(getClass(), String.format("[%s][%s] Added task. [name=%s][type=%s]"
                        , namespace, name, task.name(), task.getClass().getCanonicalName()));
            } else {
                throw new ConfigurationException(String.format("Invalid Task class specified. " +
                        "[type=%s]", tc.getCanonicalName()));
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException(e);
        }
    }

    public List<TaskExecutionRecord<T>> run(@Nonnull TaskContext context,
                                            @Nonnull Principal user) throws FlowTaskException {
        Preconditions.checkArgument(queueNames.size() == 1);
        try {
            state.checkState(getClass(), ETaskGroupState.Running);
            AbstractQueue<C, M> dataSource = getDataSource(null);
            if (dataSource == null) {
                throw new FlowTaskException("Default Data Source not registered.");
            }
            try {
                return run(context, dataSource, user);
            } finally {
                dataSource.close();
            }
        } catch (Throwable ex) {
            LogUtils.error(getClass(), ex);
            throw new FlowTaskException(ex);
        }
    }

    public List<TaskExecutionRecord<T>> run(@Nonnull TaskContext context,
                                            @Nonnull String dataSourceName,
                                            @Nonnull Principal user) throws FlowTaskException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dataSourceName));
        try {
            state.checkState(getClass(), ETaskGroupState.Running);
            AbstractQueue<C, M> dataSource = getDataSource(dataSourceName);
            if (dataSource == null) {
                throw new FlowTaskException(String.format("Specified Data Source not registered. " +
                        "[name=%s]", dataSourceName));
            }
            try {
                return run(context, dataSource, user);
            } finally {
                dataSource.close();
            }
        } catch (Throwable ex) {
            LogUtils.error(getClass(), ex);
            throw new FlowTaskException(ex);
        }
    }

    private AbstractQueue<C, M> getDataSource(String name) throws DataStoreException {
        Preconditions.checkState(QueueManager.get() != null);
        if (Strings.isNullOrEmpty(name)) {
            if (queueNames.size() > 1) {
                throw new DataStoreException(
                        String.format("[%s][%s] Multiple data sources loaded. Please specify the data source name.",
                                namespace(), name()));
            }
            String ds = queueNames.get(0);
            return QueueManager.get().getQueue(ds, connectionType, messageType);
        } else {
            return QueueManager.get().getQueue(name, connectionType, messageType);
        }
    }

    public List<TaskExecutionRecord<T>> run(@Nonnull TaskContext context,
                                            AbstractQueue<C, M> dataSource,
                                            @Nonnull Principal user) throws FlowTaskException {
        try {
            state.checkState(getClass(), ETaskGroupState.Running);
            Lock __lock = runLock.readLock();
            try {
                preRunSetup(context, dataSource);
                context = setupRunContext(context, dataSource);
                context.setTaskId(taskId);
                context.setRunId(UUID.randomUUID().toString());
                context.setTaskStartTime(System.currentTimeMillis());

                List<TaskExecutionRecord<T>> responses = new ArrayList<>();
                Collection<T> data = null;
                try {
                    LogUtils.debug(getClass(), String.format("Running Task Group. [name=%s]", name));
                    LogUtils.debug(getClass(), context);
                    __lock.lock();
                    try {
                        data = fetch(context, dataSource);
                    } finally {
                        __lock.unlock();
                    }

                    if (data != null && data.size() > 0) {
                        LogUtils.info(getClass(), String.format("Fetched data records. [count=%d]",
                                data.size()));
                        for (T record : data) {
                            TaskContext lc = setupRunContext(context, dataSource);
                            TaskExecutionRecord<T> rr = new TaskExecutionRecord<>();
                            rr.setStartTime(System.currentTimeMillis());
                            rr.setContext(lc);

                            for (AbstractFlowTask<K, T> task : tasks) {
                                TaskResponse response = task.run(record, lc, user);
                                try {
                                    if (response.getState() == ETaskResponse.Error) {
                                        LogUtils.error(getClass(), response.getError());
                                        if (errorHandler != null) {
                                            errorHandler.handleError(lc, response, record, user);
                                        }
                                        throw response.getError();
                                    } else if (response.getState() == ETaskResponse.ContinueWithError) {
                                        LogUtils.error(getClass(), String.format("[RUNID=%s][name=%s" +
                                                        "][task=%s] " +
                                                        "Continue with error --> %s", taskId, name,
                                                task.name(),
                                                response.getNonFatalError().getLocalizedMessage()));
                                        LogUtils.error(getClass(), response.getNonFatalError());
                                        if (errorHandler != null) {
                                            errorHandler.handleError(lc, response, record, user);
                                        }
                                    } else if (response.getState() == ETaskResponse.StopWithError) {
                                        LogUtils.error(getClass(), String.format("[RUNID=%s][name=%s" +
                                                        "][task=%s] " +
                                                        "Stopping with error --> %s", taskId, name,
                                                task.name(),
                                                response.getNonFatalError().getLocalizedMessage()));
                                        LogUtils.error(getClass(), response.getNonFatalError());
                                        if (errorHandler != null) {
                                            errorHandler.handleError(lc, response, record, user);
                                        }
                                        break;
                                    } else if (response.getState() == ETaskResponse.Stop) {
                                        LogUtils.info(getClass(), String.format("[RUNID=%s][name=%s" +
                                                        "][task=%s] " +
                                                        "Stopping execution chain.", taskId, name,
                                                task.name()));
                                        break;
                                    } else {
                                        LogUtils.debug(getClass(), String.format("[RUNID=%s][name=%s" +
                                                        "][task=%s] " +
                                                        "Task completed successfully.", taskId, name,
                                                task.name()));
                                    }
                                } finally {
                                    rr.setEndTime(System.currentTimeMillis());
                                    rr.addResponse(task.name(), response);
                                }
                            }
                            responses.add(rr);
                        }
                    } else {
                        LogUtils.warn(getClass(), String.format("[%s][%s] No data fetched for " +
                                "current run...", namespace, name));
                    }
                } finally {
                    LogUtils.debug(getClass(), "Calling post run handler...");
                    postRunHandler(context, data, responses, dataSource);
                }
                return responses;
            } finally {
                context.setTaskEndTime(System.currentTimeMillis());
            }
        } catch (Throwable ex) {
            LogUtils.error(getClass(), ex);
            throw new FlowTaskException(ex);
        }
    }

    public void dispose() {
        LogUtils.warn(getClass(), String.format("Disposing Task Group. [RUNID=%s][name=%s]",
                taskId, name));
        Lock __lock = runLock.writeLock();
        __lock.lock();
        try {
            if (state.getState() == ETaskGroupState.Running) {
                state.setState(ETaskGroupState.Stopped);
                LogUtils.warn(getClass(), String.format("Disposed Task Group. [RUNID=%s][name=%s]",
                        taskId, name));
            } else {
                LogUtils.warn(getClass(), String.format("Task Group not running. " +
                        "[RUNID=%s][name=%s][state=%s]", taskId, name, state.getState().name()));
            }
        } finally {
            __lock.unlock();
        }
    }

    public int recover() throws FlowTaskException {
        try {
            int count = 0;
            for (String dataSourceName : queueNames) {
                AbstractQueue<C, M> dataStore = getDataSource(dataSourceName);
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

    /**
     * Setup delegate to customize instance.
     *
     * @param node - Configuration node.
     * @throws ConfigurationException
     */
    protected abstract void setup(@Nonnull AbstractConfigNode node) throws ConfigurationException;

    /**
     * Fetch the next set of data records to be processed.
     *
     * @param context    - Run context handle.
     * @param dataSource - Data Source instance to fetch data from.
     * @return - List of fetched records. (NULL if not records to process)
     * @throws FlowTaskException
     */
    protected abstract Collection<T> fetch(@Nonnull TaskContext context,
                                     @Nonnull AbstractQueue<C, M> dataSource) throws FlowTaskException;

    /**
     * Recover any pending processing messages.
     *
     * @param dataSource - Data Source instance to fetch data from.
     * @return - List of fetched records. (NULL if not records to process)
     * @throws FlowTaskException
     */
    protected abstract List<T> recover(@Nonnull AbstractQueue<C, M> dataSource) throws FlowTaskException;

    /**
     * Delegate method to be called before every run batch.
     *
     * @param context   - Task Context.
     * @param dataStore - Data Store handle.
     * @throws FlowTaskException
     */
    protected abstract void preRunSetup(@Nonnull TaskContext context,
                                        @Nonnull AbstractQueue<C, M> dataStore) throws FlowTaskException;

    /**
     * Setup the run context for this run cycle.
     *
     * @param context    - Source context handle.
     * @param dataSource - Data Source instance to fetch data from.
     * @return - Run context handle.
     * @throws FlowTaskException
     */
    protected abstract TaskContext setupRunContext(@Nonnull TaskContext context,
                                                   @Nonnull AbstractQueue<C, M> dataSource) throws FlowTaskException;

    /**
     * Delegate method is called at the end of each run. The base method doesn't do
     * anything.
     *
     * @param data       - List of data records fetched for this run.
     * @param records    - Execution response list.
     * @param dataSource - Data Source instance to fetch data from.
     * @throws FlowTaskException
     */
    protected abstract void postRunHandler(@Nonnull TaskContext taskContext,
                                           Collection<T> data,
                                           List<TaskExecutionRecord<T>> records,
                                           @Nonnull AbstractQueue<C, M> dataSource) throws FlowTaskException;
}
