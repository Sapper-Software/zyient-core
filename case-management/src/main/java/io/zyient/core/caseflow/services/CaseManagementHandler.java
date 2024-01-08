package io.zyient.core.caseflow.services;

import com.google.common.base.Preconditions;
import io.zyient.base.common.AbstractState;
import io.zyient.base.common.StateException;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.ServiceHandler;
import io.zyient.base.core.model.UserOrRole;
import io.zyient.core.caseflow.CaseManager;
import io.zyient.core.caseflow.errors.CaseActionException;
import io.zyient.core.caseflow.errors.CaseAuthorizationError;
import io.zyient.core.caseflow.model.Case;
import io.zyient.core.caseflow.model.CaseDocument;
import io.zyient.core.caseflow.model.CaseId;
import io.zyient.core.caseflow.model.CaseState;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.model.DocumentState;
import io.zyient.core.sdk.model.caseflow.CaseEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class CaseManagementHandler<P extends Enum<P>, S extends CaseState<P>, E extends DocumentState<?>, T extends CaseDocument<E, T>, N extends Enum<N>> implements ServiceHandler<N> {
    protected String configPath;
    protected EConfigFileType configFileType;
    private String passKey;
    protected CaseManager<P, S, E, T> caseManager;
    protected BaseEnv<N> env;

    public CaseManagementHandler<P, S, E, T, N> withPassKey(@NonNull String passKey) {
        this.passKey = passKey;
        return this;
    }

    @Override
    public ServiceHandler<N> setConfigFile(@NonNull String configPath) {
        this.configPath = configPath;
        return this;
    }

    @Override
    public ServiceHandler<N> setConfigSource(@NonNull String configFileType) {
        this.configFileType = EConfigFileType.parse(configFileType);
        return this;
    }

    /**
     * @Override public ServiceHandler<EApEnvState> init() throws Exception {
     * if (Strings.isNullOrEmpty(configPath)) {
     * throw new Exception("Configuration file path not specified...");
     * }
     * if (configFileType == null) {
     * configFileType = EConfigFileType.File;
     * }
     * XMLConfiguration configuration = ConfigReader.read(configPath, configFileType);
     * env = (ApEnv) BaseEnv.create(ApEnv.class, configuration, passKey);
     * caseManager = env.caseManager();
     * if (caseManager == null) {
     * throw new Exception(String.format("Invalid configuration: Case manager not initialized. [path=%s]",
     * configPath));
     * }
     * return this;
     * }
     */


    @Override
    public ServiceHandler<N> stop() throws Exception {
        if (env != null) {
            env.close();
            BaseEnv.remove(env.name());
        }
        return this;
    }

    @Override
    public AbstractState<N> status() {
        Preconditions.checkNotNull(env);
        return env.state();
    }

    @Override
    public String name() {
        Preconditions.checkNotNull(env);
        return env.name();
    }

    @Override
    public void checkState() throws StateException {
        Preconditions.checkNotNull(env);
        if (!env.state().isAvailable()) {
            throw new StateException(String.format("Service not available. [state=%s]",
                    env.state().getState().name()));
        }
    }

    @SuppressWarnings("unchecked")
    public List<CaseEntity<P>> findByState(@NonNull List<? extends Enum<?>> states,
                                           @NonNull Class<? extends Case<P, S, E, T>> caseType,
                                           Map<String, Boolean> sort,
                                           int currentPage,
                                           int batchSize,
                                           boolean fetchDocuments,
                                           @NonNull UserOrRole caller) throws CaseActionException, CaseAuthorizationError, StateException {
        checkState();
        Preconditions.checkArgument(!states.isEmpty());
        try {
            List<CaseEntity<P>> cases = null;
            Map<String, Object> params = new HashMap<>();
            StringBuilder condition = new StringBuilder("(");
            int count = 0;
            for (Enum<?> state : states) {
                if (count > 0) {
                    condition.append(" OR ");
                }
                String k = String.format("state_%d", count);
                condition.append(String.format("caseState.state = :%s", k));
                params.put(k, state);
                count++;
            }
            condition.append(")");
            AbstractDataStore.Q query = new AbstractDataStore.Q()
                    .where(condition.toString())
                    .addAll(params);
            if (sort != null && !sort.isEmpty()) {
                query.sort(sort);
            }
            List<Case<P, S, E, T>> records = (List<Case<P, S, E, T>>) caseManager.search(query,
                    caseType,
                    currentPage,
                    batchSize,
                    fetchDocuments,
                    caller,
                    null);
            if (records != null && !records.isEmpty()) {
                cases = new ArrayList<>(records.size());
                for (Case<P, S, E, T> record : records) {
                    cases.add(record.as());
                }
            }
            return cases;
        } catch (CaseAuthorizationError | CaseActionException e) {
            throw e;
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            throw new CaseActionException(t);
        }
    }

    @SuppressWarnings("unchecked")
    public List<CaseEntity<P>> findAssignedTo(@NonNull UserOrRole assignedTo,
                                              List<? extends Enum<?>> states,
                                              @NonNull Class<? extends Case<P, S, E, T>> caseType,
                                              Map<String, Boolean> sort,
                                              int currentPage,
                                              int batchSize,
                                              boolean fetchDocuments,
                                              @NonNull UserOrRole caller) throws CaseActionException, CaseAuthorizationError, StateException {
        checkState();
        try {
            List<CaseEntity<P>> cases = null;
            StringBuilder condition = new StringBuilder("assignedTo.name = :name");
            Map<String, Object> params = Map.of("name", assignedTo.getName());
            if (states != null && !states.isEmpty()) {
                params = new HashMap<>(params);
                condition = new StringBuilder(String.format("%s AND (", condition.toString()));
                int count = 0;
                for (Enum<?> state : states) {
                    if (count > 0) {
                        condition.append(" OR ");
                    }
                    String k = String.format("state_%d", count);
                    condition.append(String.format("caseState = :%s", k));
                    params.put(k, state.name());
                    count++;
                }
                condition.append(")");
            }
            AbstractDataStore.Q query = new AbstractDataStore.Q()
                    .where(condition.toString())
                    .addAll(params);
            if (sort != null && !sort.isEmpty()) {
                query.sort(sort);
            }
            List<Case<P, S, E, T>> records = (List<Case<P, S, E, T>>) caseManager.search(query,
                    caseType,
                    currentPage,
                    batchSize,
                    fetchDocuments,
                    caller,
                    null);
            if (records != null && !records.isEmpty()) {
                cases = new ArrayList<>(records.size());
                for (Case<P, S, E, T> record : records) {
                    cases.add(record.as());
                }
            }

            return cases;
        } catch (CaseAuthorizationError | CaseActionException e) {
            throw e;
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            throw new CaseActionException(t);
        }
    }

    public CaseEntity<P> assignTo(@NonNull String caseId,
                                  @NonNull UserOrRole assignTo,
                                  @NonNull UserOrRole assigner,
                                  @NonNull String comments) throws CaseAuthorizationError, CaseActionException, StateException {
        checkState();
        try {
            Case<P, S, E, T> caseObject = caseManager().assignTo(caseId,
                    assignTo,
                    comments,
                    assigner,
                    null);
            return caseObject.as();
        } catch (CaseAuthorizationError | CaseActionException e) {
            throw e;
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            throw new CaseActionException(t);
        }
    }

    public CaseEntity<P> removeAssignment(@NonNull String caseId,
                                          @NonNull UserOrRole assigner,
                                          @NonNull String comments) throws CaseAuthorizationError, CaseActionException, StateException {
        checkState();
        try {
            Case<P, S, E, T> caseObject = caseManager().assignTo(caseId,
                    null,
                    comments,
                    assigner,
                    null);
            return caseObject.as();
        } catch (CaseAuthorizationError | CaseActionException e) {
            throw e;
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            throw new CaseActionException(t);
        }
    }

    public CaseEntity<P> updateCaseState(@NonNull String caseId,
                                         @NonNull Class<? extends Case<P, S, E, T>> caseType,
                                         @NonNull P updatedState,
                                         @NonNull String comments,
                                         @NonNull UserOrRole modifier) throws CaseActionException, CaseAuthorizationError, StateException {
        checkState();
        try {
            Case<P, S, E, T> caseObject = caseManager().updateCaseState(new CaseId(caseId),
                    caseType,
                    updatedState,
                    comments,
                    modifier,
                    null);
            return caseObject.as();
        } catch (CaseAuthorizationError | CaseActionException e) {
            throw e;
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            throw new CaseActionException(t);
        }
    }
}
