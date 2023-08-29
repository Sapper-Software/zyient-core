package ai.sapper.cdc.intake.flow;

import ai.sapper.cdc.common.config.ConfigPath;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.stores.AbstractDataStore;
import ai.sapper.cdc.core.stores.DataStoreException;
import ai.sapper.cdc.core.stores.DataStoreManager;
import ai.sapper.cdc.intake.TaskAuditManagerSettings;
import ai.sapper.cdc.intake.flow.datastore.TaskGroup;
import com.codekutter.common.stores.ConnectionManager;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.stores.impl.HibernateConnection;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.ingestion.common.flow.datastore.TaskGroup;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.hibernate.Session;
import org.hibernate.Transaction;

import javax.annotation.Nonnull;
import javax.persistence.Query;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class TaskAuditManager {
    private boolean audited = false;
    private TaskAuditManagerSettings settings;
    private DataStoreManager storeManager;

    public TaskAuditRecord create(@Nonnull TaskGroup<?, ?, ?> taskGroup,
                                  @Nonnull TaskContext context,
                                  @Nonnull String source,
                                  Map<String, String> params) throws DataStoreException {
        try {
            if (audited) {
                AbstractDataStore<TaskAuditRecord> dataStore = storeManager()
                        .getDataStore(settings.getDataStore(), settings().getDataStoreType());
                if (dataStore == null) {
                    throw new DataStoreException(
                            String.format("DataStore not found. [connection=%s]", settings.getDataStore()));
                }
                TaskAuditId id = new TaskAuditId();
                id.setTaskId(context.getRunId());
                TaskAuditRecord record = new TaskAuditRecord();
                record.setTaskId(id);
                record.setTaskGroup(taskGroup.namespace());
                record.setTaskName(taskGroup.name());
                record.setSource(source);
                record.setCorrelationId(context.getCorrelationId());
                record.setStartTime(System.currentTimeMillis());
                record.setTaskState(ETaskResponse.Running);

                if (params != null && !params.isEmpty()) {
                    for (String key : params.keySet()) {
                        record.addParam(key, params.get(key));
                    }
                }
                try {
                    record = dataStore.create(record, TaskAuditRecord.class, null);
                } finally {
                    dataStore.close();
                }
                return record;
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public TaskAuditRecord finish(@Nonnull TaskContext context, Map<String, String> params) throws DataStoreException {
        if (audited) {
            try {
                HibernateConnection connection = (HibernateConnection) ConnectionManager.get().connection(dbConnection,
                        Session.class);
                if (connection == null) {
                    throw new DataStoreException(
                            String.format("DB Connection not found. [connection=%s]", dbConnection));
                }
                Session session = connection.connection();
                try {
                    TaskAuditRecord record = find(context.getRunId(), session);
                    if (record == null) {
                        throw new DataStoreException(
                                String.format("Task Audit record not found. [id=%s]", context.getRunId()));
                    }
                    record.setStep(null);
                    record.setStepUpdateTimestamp(0);
                    record.setError(null);
                    record.setTaskState(ETaskResponse.OK);
                    record.setEndTime(System.currentTimeMillis());

                    if (params != null && !params.isEmpty()) {
                        for (String key : params.keySet()) {
                            record.addParam(key, params.get(key));
                        }
                    }
                    Transaction tx = connection.startTransaction();
                    try {
                        session.update(record);
                        tx.commit();
                    } catch (Exception e) {
                        tx.rollback();
                        throw e;
                    }
                    return record;
                } finally {
                    connection.close(session);
                }
            } catch (Exception e) {
                throw new DataStoreException(e);
            }
        }

        return null;
    }

    public TaskAuditRecord step(@Nonnull TaskContext context, @Nonnull String step) throws DataStoreException {
        if (audited) {
            try {
                HibernateConnection connection = (HibernateConnection) ConnectionManager.get().connection(dbConnection,
                        Session.class);
                if (connection == null) {
                    throw new DataStoreException(
                            String.format("DB Connection not found. [connection=%s]", dbConnection));
                }
                Session session = connection.connection();
                try {
                    TaskAuditRecord record = find(context.getRunId(), session);
                    if (record == null) {
                        throw new DataStoreException(
                                String.format("Task Audit record not found. [id=%s]", context.getRunId()));
                    }
                    record.setStep(step);
                    record.setStepUpdateTimestamp(System.currentTimeMillis());

                    Transaction tx = connection.startTransaction();
                    try {
                        session.update(record);
                        tx.commit();
                    } catch (Exception e) {
                        tx.rollback();
                        throw e;
                    }
                    return record;
                } finally {
                    connection.close(session);
                }
            } catch (Exception e) {
                throw new DataStoreException(e);
            }
        }

        return null;
    }

    public TaskAuditRecord find(@Nonnull TaskContext context) throws DataStoreException {
        return find(context.getRunId());
    }

    public TaskAuditRecord find(@Nonnull String taskId) throws DataStoreException {
        try {
            if (audited) {
                HibernateConnection connection = (HibernateConnection) ConnectionManager.get().connection(dbConnection,
                        Session.class);
                if (connection == null) {
                    throw new DataStoreException(
                            String.format("DB Connection not found. [connection=%s]", dbConnection));
                }
                TaskAuditRecord record = null;
                Session session = connection.connection();
                try {
                    record = find(taskId, session);
                } finally {
                    connection.close(session);
                }
                return record;
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private TaskAuditRecord find(String taskId, Session session) throws DataStoreException {
        TaskAuditRecord record = null;
        try {
            TaskAuditId id = new TaskAuditId();
            id.setTaskId(taskId);

            record = session.find(TaskAuditRecord.class, id);
        } catch (Exception ex) {
            throw ex;
        }

        return record;
    }

    public TaskAuditRecord findByCorrelationId(@Nonnull String correlationId) throws DataStoreException {
        try {
            if (audited) {
                HibernateConnection connection = (HibernateConnection) ConnectionManager.get().connection(dbConnection,
                        Session.class);
                if (connection == null) {
                    throw new DataStoreException(
                            String.format("DB Connection not found. [connection=%s]", dbConnection));
                }
                TaskAuditRecord record = null;
                Session session = connection.connection();
                try {
                    try {
                        String qstr = String.format("FROM %s WHERE correlationId = :c_id",
                                TaskAuditRecord.class.getCanonicalName());
                        Query query = (Query) session.createQuery(qstr);
                        query.setParameter("c_id", correlationId);
                        List<TaskAuditRecord> records = query.getResultList();
                        if (records != null && !records.isEmpty()) {
                            record = records.get(0);
                        }
                    } catch (Exception ex) {
                        throw ex;
                    }
                } finally {
                    connection.close(session);
                }
                return record;
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public List<TaskAuditRecord> findByParams(@Nonnull Map<String, String> params, boolean isAndJoin)
            throws DataStoreException {
        Preconditions.checkArgument(params != null && !params.isEmpty());
        try {
            if (audited) {
                HibernateConnection connection = (HibernateConnection) ConnectionManager.get().connection(dbConnection,
                        Session.class);
                if (connection == null) {
                    throw new DataStoreException(
                            String.format("DB Connection not found. [connection=%s]", dbConnection));
                }
                Session session = connection.connection();
                try {
                    StringBuilder where = new StringBuilder();

                    for (int ii = 0; ii < params.size(); ii++) {
                        if (ii > 0) {
                            if (isAndJoin) {
                                where.append(" AND ");
                            } else {
                                where.append(" OR ");
                            }
                        }
                        where.append("params.id.param = ").append(":param_").append(ii);
                    }
                    String qstr = String.format("FROM %s AS records INNER JOIN records.params AS params  WHERE (%s)", TaskAuditRecord.class.getCanonicalName(),
                            where.toString());
                    LogUtils.debug(getClass(), String.format("PARAM QUERY=[%s]", qstr));
                    Query query = (Query) session.createQuery(qstr);
                    int count = 0;
                    for (String key : params.keySet()) {
                        query.setParameter("param_" + count, params.get(key));
                        count++;
                    }
                    List<TaskAuditRecord> records = query.getResultList();
                    if (records != null && !records.isEmpty()) {
                        return records;
                    }
                } finally {
                    connection.close(session);
                }
            }
            return null;
        } catch (Exception e) {
            throw new DataStoreException(e);
        }
    }

    public TaskAuditRecord error(@Nonnull String taskId, Throwable error) throws DataStoreException {
        if (audited) {
            try {
                HibernateConnection connection = (HibernateConnection) ConnectionManager.get().connection(dbConnection,
                        Session.class);
                if (connection == null) {
                    throw new DataStoreException(
                            String.format("DB Connection not found. [connection=%s]", dbConnection));
                }
                Session session = connection.connection();
                try {
                    TaskAuditRecord record = find(taskId, session);
                    if (record == null) {
                        throw new DataStoreException(String.format("Task Audit record not found. [id=%s]", taskId));
                    }
                    record.setTaskState(ETaskResponse.Error);
                    String mesg = error.getLocalizedMessage();
                    if (LogUtils.isDebugEnabled()) {
                        String st = LogUtils.getStackTrace(error);
                        mesg = String.format("%s\n%s", error.getLocalizedMessage(), st);
                    }
                    record.setError(mesg);
                    record.setEndTime(System.currentTimeMillis());

                    Transaction tx = connection.startTransaction();
                    try {
                        session.update(record);
                        tx.commit();
                    } catch (Exception e) {
                        tx.rollback();
                        throw e;
                    }
                    return record;
                } finally {
                    connection.close(session);
                }
            } catch (Exception e) {
                throw new DataStoreException(e);
            }
        }
        return null;
    }

    public void configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                          @Nonnull DataStoreManager storeManager) throws ConfigurationException {
        try {
            String path = ConfigReader.getPathAnnotation(TaskAuditManagerSettings.class);
            if (!Strings.isNullOrEmpty(path) && ConfigReader.checkIfNodeExists(xmlConfig, path)) {
                ConfigReader reader = new ConfigReader(xmlConfig, path, TaskAuditManagerSettings.class);
                reader.read();
                settings = (TaskAuditManagerSettings) reader.settings();
                audited = true;
                this.storeManager = storeManager;
                DefaultLogger.info("Task Audit manager initialized...");
            } else {
                audited = false;
            }
        } catch (Exception ex) {
            audited = false;
            throw new ConfigurationException(ex);
        }
    }

    private static final TaskAuditManager __manager = new TaskAuditManager();

    public static TaskAuditManager get() {
        return __manager;
    }

    public static boolean isAudited() {
        return __manager.audited;
    }

    public static void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                            @Nonnull DataStoreManager storeManager) throws ConfigurationException {
        __manager.configure(xmlConfig, storeManager);
    }
}
