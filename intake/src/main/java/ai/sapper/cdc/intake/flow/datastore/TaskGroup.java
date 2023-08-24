package ai.sapper.cdc.intake.flow.datastore;

import ai.sapper.cdc.common.config.ConfigPath;
import com.codekutter.common.model.ConnectionConfig;
import com.codekutter.common.stores.*;
import com.codekutter.common.stores.impl.HibernateConnection;
import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.TypeUtils;
import com.codekutter.zconfig.common.*;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigListNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.transformers.StringListParser;
import com.codekutter.zconfig.common.transformers.URLEncodedParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.ingestion.common.flow.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.hibernate.Session;

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
public abstract class TaskGroup<K, T, C> implements IConfigurable {
	@Getter
	@Setter
	@Accessors(fluent = true)
	@ConfigPath(path = "connection")
	public static class ConnectionConfigSettings<C> {
		@ConfigAttribute(required = true)
		private Class<? extends ConnectionConfig> config;
		@ConfigAttribute(required = true)
		private Class<? extends AbstractConnection<C>> type;
		@ConfigValue(parser = URLEncodedParser.class)
		private String filter;
	}

	public static final String CONFIG_NODE_TASKS = "tasks";
	public static final String CONFIG_NODE_DATA_SOURCES = "dataSources";
	public static final long DEFAULT_STORE_REFRESH_INTERVAL = 30 * 60 * 1000; // 30 mins.
	@ConfigAttribute(name = "name", required = true)
	private String name;
	@ConfigAttribute(name = "namespace", required = true)
	private String namespace;
	@ConfigAttribute(name = "dataSources@dynamic")
	private boolean dynamicStores = false;
	@ConfigValue(name = "dataSources/filter", parser = URLEncodedParser.class)
	private String filter;
	@ConfigAttribute(name = "dataSources@dbconnection")
	private String dbConnectionName;
	@ConfigAttribute(name = "dataSources@configType")
	private Class<? extends DataStoreConfig> configType;
	@ConfigValue(name = "dataSources/refreshInterval")
	private long storeRefreshInterval;
	@Setter(AccessLevel.NONE)
	private long lastRefreshed = 0;
	@ConfigValue(name = "dataSources", parser = StringListParser.class)
	private List<String> dataSourceNames;
	@ConfigValue(name = "allowConcurrentPerStore")
	private boolean allowConcurrentPerStore = true;
	@ConfigValue
	private boolean audited = true;
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
	private Class<? extends T> entityType;
	@Setter(AccessLevel.NONE)
	private Class<? extends AbstractDataStore<C>> dataStoreType;
    @Setter(AccessLevel.NONE)
    private DataStoreManager dataStoreManager;
	@SuppressWarnings("rawtypes")
	@Setter(AccessLevel.NONE)
	private ConnectionConfigSettings settings = null;
	@Setter(AccessLevel.NONE)
	@Getter(AccessLevel.NONE)
	private Map<String, ReentrantLock> storeRunLocks = new HashMap<>();

	public TaskGroup(@Nonnull Class<? extends T> entityType,
			@Nonnull Class<? extends AbstractDataStore<C>> dataStoreType) {
		this.entityType = entityType;
		this.dataStoreType = dataStoreType;
		if (storeRefreshInterval == 0L) {
			storeRefreshInterval = DEFAULT_STORE_REFRESH_INTERVAL;
			try{
			this.dataStoreManager = IntakeProcessingEnv.env().getEntityManager()
					.dataStoreManager();
			}catch (EnvException ex){
				LogUtils.error(getClass(), String.format("TaskGroup() throws [%s]", ex.getMessage()));
			}
		}
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

	@SuppressWarnings("unchecked")
	public List<String> fetchDynamicStores() throws ConfigurationException {
		Preconditions.checkState(dynamicStores = true);
		Preconditions.checkState(settings != null);
		runLock.writeLock().lock();
		try {
			if ((System.currentTimeMillis() - lastRefreshed) < storeRefreshInterval)
				return dataSourceNames;

			HibernateConnection connection = (HibernateConnection) ConnectionManager.get().connection(dbConnectionName,
					Session.class);
			if (connection == null) {
				throw new ConfigurationException(String.format("DB Connection not found. [name=%s]", dbConnectionName));
			}

			Session session = connection.connection();
			try {
				LogUtils.debug(getClass(), String.format("Connection filter [%s]", settings.filter));
				List<AbstractCollection<C>> connections = ConnectionManager.get().readConnections(settings.config,
						session, settings.filter);
				if (connections == null || connections.isEmpty()) {
					throw new ConfigurationException(
							String.format("No connections loaded. [type=%s]", settings.type.getCanonicalName()));
				}
				LogUtils.debug(getClass(), String.format("DataStore filter [%s]", filter));
				Set<String> set = dataStoreManager().readDynamicDConfig(session, dataStoreType, configType, filter);
				if (set != null && !set.isEmpty()) {
					dataSourceNames = new ArrayList<>(set);
				} else {
					throw new ConfigurationException("No data stores found...");
				}
				lastRefreshed = System.currentTimeMillis();
				for (String ds : dataSourceNames) {
					if (!storeRunLocks.containsKey(ds)) {
						storeRunLocks.put(ds, new ReentrantLock());
					}
				}
				return dataSourceNames;
			} finally {
				connection.close(session);
			}
		} catch (Exception ex) {
			throw new ConfigurationException(ex);
		} finally {
			runLock.writeLock().unlock();
		}
	}

	@Override
	public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
		Preconditions.checkArgument(node instanceof ConfigPathNode);
		Lock __lock = runLock.writeLock();
		__lock.lock();
		try {
			ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
			if (Strings.isNullOrEmpty(name)) {
				throw ConfigurationException.propertyNotFoundException("name");
			}
			instanceId = String.format("%s-%s", name, instanceId);
			LogUtils.warn(getClass(), String.format("Configuring Task Group. " + "[GROUP ID=%s][type=%s][path=%s]",
					instanceId, getClass().getCanonicalName(), node.getAbsolutePath()));
			AbstractConfigNode tnode = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
			if (!(tnode instanceof ConfigPathNode)) {
				throw new ConfigurationException(String.format("Configuration node not found. " + "[node=%s][path=%s]",
						ConfigUtils.getAnnotationPath(getClass()), node.getAbsolutePath()));
			}
			if (dynamicStores) {
				if (Strings.isNullOrEmpty(dbConnectionName)) {
					throw ConfigurationException.propertyNotFoundException("DB CONNECTION NAME");
				}
				if (configType == null) {
					throw ConfigurationException.propertyNotFoundException("CONFIGURATION CLASS");
				}
				AbstractConfigNode snode = tnode.find(CONFIG_NODE_DATA_SOURCES);
				if (!(snode instanceof ConfigPathNode)) {
					throw new ConfigurationException(String
							.format("Data Stores configuration node not found. [path=%s]", tnode.getAbsolutePath()));
				}
				settings = new ConnectionConfigSettings<C>();
				ConfigurationAnnotationProcessor.readConfigAnnotations(settings.getClass(), (ConfigPathNode) snode,
						settings);
				List<String> stores = fetchDynamicStores();
				for (String store : stores) {
					LogUtils.info(getClass(), String.format("Configured Dynamic Data Store: [%s]", store));
				}
			} else {
				if (dataSourceNames == null || dataSourceNames.isEmpty()) {
					throw new ConfigurationException(
							String.format("No data sources specified. [path=%s]", node.getAbsolutePath()));
				}
				for (String store : dataSourceNames) {
					LogUtils.info(getClass(), String.format("Configured Data Store: [%s]", store));
					storeRunLocks.put(store, new ReentrantLock());
				}
			}
			tasks = new ArrayList<>();
			configTasks((ConfigPathNode) tnode, tasks);
			if (tasks.isEmpty()) {
				throw new ConfigurationException(String.format("No tasks defined. [path=%s]", node.getAbsolutePath()));
			}
			configErrorHandler((ConfigPathNode) tnode);
			setup(tnode);

			state.setState(ETaskGroupState.Running);
			LogUtils.warn(getClass(),
					String.format("Configured Task Group. " + "[GROUP ID=%s][type=%s][path=%s][name=%s]", instanceId,
							getClass().getCanonicalName(), node.getAbsolutePath(), name));
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
			throw new ConfigurationException(
					String.format("Error handler class not found. " + "[path=%s]", node.getAbsolutePath()));
		}
		try {
			Class<? extends TaskFlowErrorHandler<T>> tc = (Class<? extends TaskFlowErrorHandler<T>>) Class.forName(cls);
			errorHandler = TypeUtils.createInstance(tc);
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			throw new ConfigurationException(e);
		} catch (Exception e) {
			LogUtils.error(getClass(), e);
			throw new ConfigurationException(e);
		}
	}

	public void configTasks(ConfigPathNode node, List<AbstractFlowTask<K, T>> tasks) throws ConfigurationException {
		AbstractConfigNode tsnode = node.find(CONFIG_NODE_TASKS);
		if (tsnode != null) {
			String tname = ConfigUtils.getAnnotationPath(AbstractFlowTask.class);
			Preconditions.checkArgument(!Strings.isNullOrEmpty(tname));

			if (tsnode instanceof ConfigPathNode) {
				AbstractConfigNode tnode = ConfigUtils.getPathNode(AbstractFlowTask.class, (ConfigPathNode) tsnode);
				if (tnode != null)
					configTask((ConfigPathNode) tnode, tasks);
			} else if (tsnode instanceof ConfigListNode) {
				ConfigListNode<?> clnode = (ConfigListNode<?>) tsnode;
				for (Object obj : clnode.getValues()) {
					if (obj instanceof ConfigPathNode) {
						configTask((ConfigPathNode) obj, tasks);
					} else {
						throw new ConfigurationException(String.format(
								"Invalid Configuration " + "node. [path=%s][type=%s]",
								((AbstractConfigNode) obj).getAbsolutePath(), obj.getClass().getCanonicalName()));
					}
				}
			} else {
				throw new ConfigurationException(String.format("Invalid Configuration " + "node. [path=%s][type=%s]",
						tsnode, tsnode.getClass().getCanonicalName()));
			}

		}
	}

	@SuppressWarnings("unchecked")
	private void configTask(ConfigPathNode node, List<AbstractFlowTask<K, T>> tasks) throws ConfigurationException {
		String nname = ConfigUtils.getAnnotationPath(AbstractFlowTask.class);
		Preconditions.checkArgument(node.getName().compareTo(nname) == 0);
		String cls = ConfigUtils.getClassAttribute(node);
		if (Strings.isNullOrEmpty(cls)) {
			throw new ConfigurationException(
					String.format("Task class not found. " + "[path=%s]", node.getAbsolutePath()));
		}
		try {
			Class<? extends AbstractFlowTask<K, T>> tc = (Class<? extends AbstractFlowTask<K, T>>) Class.forName(cls);
			AbstractFlowTask<K, T> task = TypeUtils.createInstance(tc);
			String name = ConfigUtils.getNameAttribute(node);
			task.name(name);
			task.withTaskGroup(this).configure(node);
			tasks.add(task);

			LogUtils.info(getClass(), String.format("[%s][%s] Added task. [name=%s][type=%s]", namespace, name,
					task.name(), task.getClass().getCanonicalName()));
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			throw new ConfigurationException(e);
		} catch (Exception e) {
			LogUtils.error(getClass(), e);
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
			LogUtils.error(getClass(), ex);
			throw new FlowTaskException(ex);
		}
	}

	public Map<String, TaskGroupResponse<T>> runAll(@Nonnull TaskContext context, @Nonnull Principal user)
			throws FlowTaskException {
		try {
			state.checkState(getClass(), ETaskGroupState.Running);
			if (dynamicStores) {
				if ((System.currentTimeMillis() - lastRefreshed) > storeRefreshInterval) {
					fetchDynamicStores();
				}
			}
			Preconditions.checkState(dataSourceNames != null && !dataSourceNames.isEmpty());

			Map<String, TaskGroupResponse<T>> results = new ConcurrentHashMap<>();

			dataSourceNames.parallelStream().forEach(store-> {
				TaskGroupResponse<T> result = null;
				try {
					LogUtils.info(getClass(),
							String.format("Dynamic TG Running Data Sources in Parallel taskGroup [%s.%s] for data source [%s]...", namespace, name, store));
					result = run(new TaskContext(), store, user);
					LogUtils.debug(getClass(), result);
					results.put(store, result);
				} catch (Exception e) {
					LogUtils.error(getClass(), e);
					if (result != null) {
						results.put(store, result);
					}
				}});

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
			LogUtils.error(getClass(), ex);
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
			LogUtils.error(getClass(), ex);
			throw new FlowTaskException(ex);
		}
	}

	private AbstractDataStore<C> getDataSource(String name) throws DataStoreException, EnvException {
		Preconditions.checkState(dataStoreManager() != null);
		if (Strings.isNullOrEmpty(name)) {
			if (dataSourceNames.size() > 1) {
				throw new DataStoreException(
						String.format("[%s][%s] Multiple data sources loaded. Please specify the data source name.",
								namespace(), name()));
			}
			String ds = dataSourceNames.get(0);
			return dataStoreManager().getDataStore(ds, dataStoreType);
		} else {
			return dataStoreManager().getDataStore(name, dataStoreType);
		}
	}

	public TaskGroupResponse<T> run(@Nonnull TaskContext context, @Nonnull AbstractDataStore<C> dataSource,
			@Nonnull Principal user) throws FlowTaskException {
		if (allowConcurrentPerStore) {
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
				LogUtils.warn(getClass(), String.format("DataStore being processed. [name=%s]", dataSource.name()));
				return null;
			}
		}
	}

	private TaskGroupResponse<T> doRun(@Nonnull TaskContext context, @Nonnull AbstractDataStore<C> dataSource,
			@Nonnull Principal user) throws FlowTaskException {
		TaskGroupResponse<T> response = new TaskGroupResponse<>();
		response.setNamespace(namespace);
		response.setName(name);
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
				LogUtils.debug(getClass(), String.format("Running Task Group. [name=%s]", name));
				// LogUtils.debug(getClass(), context);
				List<TaskExecutionRecord<T>> responses = new ArrayList<>();
				TaskAuditRecord taskAuditRecord = null;
				if (audited) {
					taskAuditRecord = TaskAuditManager.get().create(this, context, dataSource.name(),
							getStartAuditParams(context));
				}
				try {
					data = fetch(context, dataSource);

					if (data != null && data.size() > 0) {
						response.setResults(responses);
						response.setRecordCount(data.size());
						LogUtils.info(getClass(), String.format("Fetched data records. [count=%d]", data.size()));
						for (T record : data) {
							runTasks(context, dataSource, record, taskAuditRecord, response, user);
						}
						if (taskAuditRecord != null) {
							TaskAuditManager.get().finish(context, getFinishAuditParams(context));
						}
					} else {
						String mesg = String.format("[%s][%s] No data fetched for " + "current run...", namespace,
								name);
						LogUtils.warn(getClass(), mesg);
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
					LogUtils.debug(getClass(), "Calling post run handler...");
					postRunHandler(context, data, responses, dataSource);
					dataStoreManager().closeStores();
				}
			} finally {
				response.setEndTime(System.currentTimeMillis());
				context.setTaskEndTime(System.currentTimeMillis());
				__lock.unlock();
			}
		} catch (Throwable ex) {
			LogUtils.error(getClass(), ex);
			response.setError(ex);
		}
		LogUtils.debug(getClass(), response);
		return response;
	}

	public void runTasks(TaskContext context, AbstractDataStore<C> dataSource, T record,
			TaskAuditRecord taskAuditRecord, TaskGroupResponse<T> result, Principal user) throws Throwable {
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
					LogUtils.warn(getClass(),
							String.format("Stopped processing for record. [run ID=%s]", context.getRunId()));
					break;
				} else if (response.getState() == ETaskResponse.StopWithError
						|| response.getState() == ETaskResponse.MoveToError) {
					Throwable ex = response.getNonFatalError();
					if (ex == null) {
						ex = response.getError();
					}
					if (ex != null)
						LogUtils.error(getClass(), ex);
					result.setErrorCount(result.getErrorCount() + 1);
					break;
				} else if (response.getState() == ETaskResponse.Error) {
					throw new FlowTaskException(response.getError());
				}
				dataStoreManager().commit();
			} catch (Throwable e) {
				dataStoreManager().rollback();
				LogUtils.error(getClass(), e);
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
			LogUtils.error(getClass(), response.getError());
			if (errorHandler != null) {
				errorHandler.handleError(context, response, data, user);
			}
			throw response.getError();
		} else if (response.getState() == ETaskResponse.ContinueWithError) {
			LogUtils.error(getClass(),
					String.format("[GROUP ID=%s][name=%s" + "][task=%s] " + "Continue with error --> %s", instanceId,
							name, task.name(), response.getNonFatalError().getLocalizedMessage()));
			LogUtils.error(getClass(), response.getNonFatalError());
		} else if (response.getState() == ETaskResponse.StopWithError
				|| response.getState() == ETaskResponse.MoveToError) {
			LogUtils.error(getClass(),
					String.format("[GROUP ID=%s][name=%s" + "][task=%s] " + "Stopping with error --> %s", instanceId,
							name, task.name(), response.getNonFatalError().getLocalizedMessage()));
			LogUtils.error(getClass(), response.getNonFatalError());
			if (errorHandler != null) {
				errorHandler.handleError(context, response, data, user);
			}
		} else if (response.getState() == ETaskResponse.Stop) {
			LogUtils.info(getClass(),
					String.format("[GROUP ID=%s][name=%s" + "][task=%s] " + "Stopping execution chain.", instanceId,
							name, task.name()));
		} else {
			LogUtils.debug(getClass(),
					String.format("[GROUP ID=%s][name=%s" + "][task=%s] " + "Task completed successfully.", instanceId,
							name, task.name()));
		}
		return response;
	}

	public void dispose() {
		LogUtils.warn(getClass(), String.format("Disposing Task Group. [GROUP ID=%s][name=%s]", instanceId, name));
		Lock __lock = runLock.writeLock();
		__lock.lock();
		try {
			if (state.getState() == ETaskGroupState.Running) {
				state.setState(ETaskGroupState.Stopped);
				LogUtils.warn(getClass(),
						String.format("Disposed Task Group. [GROUP ID=%s][name=%s]", instanceId, name));
			} else {
				LogUtils.warn(getClass(), String.format("Task Group not running. " + "[GROUP ID=%s][name=%s][state=%s]",
						instanceId, name, state.getState().name()));
			}
		} finally {
			__lock.unlock();
		}
	}

	public int recover() throws FlowTaskException {
		try {
			int count = 0;
			for (String dataSourceName : dataSourceNames) {
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
