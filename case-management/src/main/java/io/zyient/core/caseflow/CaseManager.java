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

package io.zyient.core.caseflow;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.beans.BeanUtils;
import io.zyient.base.core.model.Actor;
import io.zyient.base.core.model.UserOrRole;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.caseflow.errors.CaseActionException;
import io.zyient.core.caseflow.errors.CaseAuthorizationError;
import io.zyient.core.caseflow.errors.CaseFatalError;
import io.zyient.core.caseflow.model.*;
import io.zyient.core.caseflow.workflow.StateTransitionHandler;
import io.zyient.core.caseflow.workflow.StateTransitionSettings;
import io.zyient.core.content.ContentProvider;
import io.zyient.core.content.DocumentContext;
import io.zyient.core.persistence.*;
import io.zyient.core.persistence.env.DataStoreEnv;
import io.zyient.core.persistence.model.DocumentId;
import io.zyient.core.persistence.model.DocumentState;
import io.zyient.core.sdk.model.caseflow.Artefact;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

@Getter
@Accessors(fluent = true)
public abstract class CaseManager<P extends Enum<P>, S extends CaseState<P>, E extends DocumentState<?>, T extends CaseDocument<E, T>>
        implements Closeable {
    private final ProcessorState state = new ProcessorState();
    private final Class<? extends CaseManagerSettings> settingsType;
    private final Class<P> caseStateType;
    private TransactionDataStore<?, ?> dataStore;
    private ContentProvider contentProvider;
    private CaseManagerSettings settings;
    private ActionAuthorization<P, S> authorization;
    private DataStoreEnv<?> env;
    private Map<String, StateTransitionHandler<P, S, E, T>> handlers;
    private Map<P, Map<P, StateTransitionHandler<P, S, E, T>>> transitions;
    private Class<? extends CaseDocument<E, T>> documentType;

    public CaseManager(@NonNull Class<? extends CaseManagerSettings> settingsType,
                       @NonNull Class<P> caseStateType) {
        this.settingsType = settingsType;
        this.caseStateType = caseStateType;
    }

    @SuppressWarnings("unchecked")
    public CaseManager<P, S, E, T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                             @NonNull DataStoreEnv<?> env) throws ConfigurationException {
        try {
            this.env = env;
            ConfigReader reader = new ConfigReader(xmlConfig,
                    CaseManagerSettings.__CONFIG_PATH,
                    settingsType);
            reader.read();
            settings = (CaseManagerSettings) reader.settings();
            DataStoreManager dataStoreManager = env.dataStoreManager();
            if (dataStoreManager == null) {
                throw new Exception(String.format("Data Store manager not specified. [env=%s]", env.name()));
            }
            dataStore = (TransactionDataStore<?, ?>) dataStoreManager.getDataStore(settings.getDataStore(),
                    settings.getDataStoreType());
            if (dataStore == null) {
                throw new Exception(String.format("Data Store not found. [name=%s][type=%s]",
                        settings.getDataStore(), settings.getDataStoreType().getCanonicalName()));
            }
            documentType = (Class<? extends CaseDocument<E, T>>) settings.getDocumentType();
            HierarchicalConfiguration<ImmutableNode> contentConfig = reader.config()
                    .configurationAt(ContentProvider.__CONFIG_PATH);
            Class<? extends ContentProvider> clazz
                    = (Class<? extends ContentProvider>) ConfigReader.readType(contentConfig);
            contentProvider = clazz.getDeclaredConstructor()
                    .newInstance()
                    .configure(reader.config(), env);
            authorization = (ActionAuthorization<P, S>) settings.getAuthorizer()
                    .getDeclaredConstructor()
                    .newInstance();
            authorization.configure(reader.config());

            readStateHandlers(reader.config());

            state.setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private void readStateHandlers(HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        if (ConfigReader.checkIfNodeExists(config, CaseManagerSettings.__CONFIG_PATH_HANDLERS)) {
            HierarchicalConfiguration<ImmutableNode> cfg
                    = config.configurationAt(CaseManagerSettings.__CONFIG_PATH_HANDLERS);
            List<HierarchicalConfiguration<ImmutableNode>> nodes
                    = cfg.configurationsAt(StateTransitionHandler.__CONFIG_PATH);
            if (nodes != null) {
                handlers = new HashMap<>();
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    Class<? extends StateTransitionHandler<P, S, E, T>> type
                            = (Class<? extends StateTransitionHandler<P, S, E, T>>) ConfigReader.readType(node);
                    StateTransitionHandler<P, S, E, T> handler = type.getDeclaredConstructor()
                            .newInstance()
                            .withCaseManager(this)
                            .configure(node, env);
                    handlers.put(handler.name(), handler);
                }
                readTransitions(config);
            }
        }
    }

    private void readTransitions(HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        if (ConfigReader.checkIfNodeExists(config, CaseManagerSettings.__CONFIG_PATH_TRANSITIONS)) {
            HierarchicalConfiguration<ImmutableNode> cfg
                    = config.configurationAt(CaseManagerSettings.__CONFIG_PATH_TRANSITIONS);
            List<HierarchicalConfiguration<ImmutableNode>> nodes
                    = cfg.configurationsAt(StateTransitionSettings.__CONFIG_PATH);
            if (nodes != null) {
                transitions = new HashMap<>();
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    ConfigReader reader = new ConfigReader(node, StateTransitionSettings.class);
                    reader.read();
                    StateTransitionSettings settings = (StateTransitionSettings) reader.settings();
                    P from = Enum.valueOf(caseStateType, settings.getFromState());
                    P to = Enum.valueOf(caseStateType, settings.getToState());
                    StateTransitionHandler<P, S, E, T> handler = handlers.get(settings.getHandler());
                    if (handler == null) {
                        throw new Exception(String.format("Specified transition handler not found. [name=%s]",
                                settings.getHandler()));
                    }
                    Map<P, StateTransitionHandler<P, S, E, T>> map = transitions.computeIfAbsent(from, k -> new HashMap<>());
                    map.put(to, handler);
                }
            }
        }
    }

    protected void checkState() throws CaseActionException {
        if (!state.isRunning()) {
            throw new CaseActionException(String.format("CaseManager: Invalid state. [state=%s]",
                    state.getState().name()));
        }
    }

    private void addCaseHistory(CaseId caseId,
                                CaseAction action,
                                CaseCode code,
                                String comment,
                                Object change,
                                UserOrRole actor) throws Exception {
        String json = null;
        if (change != null) {
            if (change instanceof String) {
                json = (String) change;
            } else
                json = JSONUtils.asString(change);
        }
        CaseHistoryId id = new CaseHistoryId();
        id.setCaseId(caseId.getId());
        id.setSequence(System.nanoTime());
        CaseHistory history = new CaseHistory();
        history.setId(id);
        history.setAction(action.getKey().getKey());
        history.setCaseCode(code.getKey().getKey());
        if (Strings.isNullOrEmpty(comment)) {
            history.setComment(code.getDescription());
        } else {
            history.setComment(comment);
        }
        history.setChange(json);
        history.setActor(new Actor(actor));
        history = dataStore.create(history, history.getClass(), null);
    }

    @SuppressWarnings("unchecked")
    public Case<P, S, E, T> create(@NonNull String name,
                                   @NonNull String description,
                                   List<Artefact> artefacts,
                                   @NonNull UserOrRole creator,
                                   @NonNull CaseCode caseCode,
                                   @NonNull String notes,
                                   Context context) throws CaseActionException,
            CaseAuthorizationError, CaseFatalError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(description));
        checkState();
        try {
            authorization.authorize(null, EStandardAction.Create.action(), creator, context);
            Case<P, S, E, T> caseObject = createInstance();
            if (caseObject.getCaseState() == null) {
                throw new Exception(String.format("Invalid case instance: state is null. [type=%s]",
                        caseObject.getClass().getCanonicalName()));
            }
            caseObject.getState().setState(EEntityState.New);
            if (caseObject.getId() == null) {
                caseObject.setId(new CaseId());
            }
            caseObject.setName(name);
            caseObject.setDescription(description);
            caseObject.setCreatedBy(new Actor(creator));
            if (context instanceof CaseContext ctx) {
                if (((CaseContext) context).customFields() != null) {
                    Map<String, Object> values = ctx.customFields();
                    for (String field : values.keySet()) {
                        Object v = values.get(field);
                        BeanUtils.setValue(caseObject, field, v);
                    }
                }
            }
            if (artefacts != null) {
                for (Artefact f : artefacts) {
                    Preconditions.checkArgument(!Strings.isNullOrEmpty(f.name()));
                    Preconditions.checkNotNull(f.file());

                    if (!f.file().exists()) {
                        throw new Exception(String.format("Artefact not found. [file=%s]", f.file().getAbsolutePath()));
                    }
                    CaseDocument<E, T> document = (CaseDocument<E, T>) settings.getDocumentType()
                            .getDeclaredConstructor()
                            .newInstance();
                    document.setId(new DocumentId(settings.getContentCollection()));
                    document.setName(f.name());
                    document.setPath(f.file());
                    document.getState().setState(EEntityState.New);
                    if (!Strings.isNullOrEmpty(f.mimeType())) {
                        document.setMimeType(f.mimeType());
                    }
                    if (!Strings.isNullOrEmpty(f.password())) {
                        document.setPassword(f.password());
                    }
                    document = validateArtefact(document);
                    caseObject.addArtefact(document);
                }
            }
            validateCase(caseObject);
            dataStore.beingTransaction();
            try {
                caseObject = save(caseObject, creator, context);
                addCaseHistory(caseObject.getId(),
                        EStandardAction.Create.action(),
                        caseCode,
                        notes,
                        caseObject,
                        creator);
                dataStore.commit();
                return caseObject;
            } catch (Throwable t) {
                dataStore.rollback(false);
                throw t;
            }
        } catch (CaseAuthorizationError | CaseActionException ae) {
            throw ae;
        } catch (Throwable ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseFatalError(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public Case<P, S, E, T> assignTo(@NonNull String caseId,
                                     UserOrRole assignTo,
                                     @NonNull String notes,
                                     @NonNull UserOrRole assigner,
                                     Context context) throws CaseActionException, CaseAuthorizationError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(notes));
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            Case<P, S, E, T> caseObject = (Case<P, S, E, T>) dataStore.find(caseId, settings.getCaseType(), context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
            Map<String, Object> diff = new HashMap<>();
            Actor current = caseObject.getAssignedTo();
            if (current == null && assignTo == null) {
                throw new CaseActionException("No assignment to remove...");
            }
            if (assignTo != null) {
                authorization.authorize(caseObject, EStandardAction.AssignTo.action(), assigner, context);
                authorization.checkAssignment(caseObject, assignTo, context);
            } else
                authorization.authorize(caseObject, EStandardAction.RemoveAssignment.action(), assigner, context);
            if (assignTo != null)
                caseObject.setAssignedTo(new Actor(assignTo));
            else
                caseObject.setAssignedTo(null);
            diff.put("assignment.removed", current);
            if (assignTo != null) {
                diff.put("assignment.to", assignTo);
            }
            validateAssignment(current, caseObject);
            caseObject.getState().setState(EEntityState.Updated);
            dataStore.beingTransaction();
            try {
                caseObject = saveUpdate(caseObject,
                        EStandardAction.AssignTo.action(),
                        EStandardCode.ActionAssignTo.code(),
                        null,
                        assigner,
                        diff,
                        context);
                dataStore.commit();
                return caseObject;
            } catch (Throwable t) {
                dataStore.rollback(false);
                throw t;
            }
        } catch (CaseAuthorizationError | CaseActionException ae) {
            throw ae;
        } catch (Throwable ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseFatalError(ex);
        }
    }


    @SuppressWarnings("unchecked")
    public Case<P, S, E, T> addArtefact(@NonNull String caseId,
                                        @NonNull Artefact artefact,
                                        @NonNull UserOrRole modifier,
                                        Context context) throws CaseAuthorizationError, CaseActionException {
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            context = AbstractDataStore.withRefresh(context);
            Case<P, S, E, T> caseObject = findById(id,
                    (Class<? extends Case<P, S, E, T>>) settings.getCaseType(),
                    true,
                    modifier,
                    context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
            authorization.authorize(caseObject, EStandardAction.AddArtefact.action(), modifier, context);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(artefact.name()));
            Preconditions.checkNotNull(artefact.file());

            if (!artefact.file().exists()) {
                throw new Exception(String.format("Artefact not found. [file=%s]", artefact.file().getAbsolutePath()));
            }
            CaseDocument<E, T> document = (CaseDocument<E, T>) settings.getDocumentType()
                    .getDeclaredConstructor()
                    .newInstance();
            document.setId(new DocumentId(settings.getContentCollection()));
            document.setName(artefact.name());
            document.setPath(artefact.file());
            document.setSourcePath(artefact.sourceUrl());
            document.getState().setState(EEntityState.New);
            if (!Strings.isNullOrEmpty(artefact.mimeType())) {
                document.setMimeType(artefact.mimeType());
            }
            if (!Strings.isNullOrEmpty(artefact.password())) {
                document.setPassword(artefact.password());
            }
            document = validateArtefact(document);
            caseObject.addArtefact(document);
            dataStore.beingTransaction();
            try {
                caseObject = saveUpdate(caseObject,
                        EStandardAction.AddArtefact.action(),
                        EStandardCode.ActionAddArtefact.code(),
                        null,
                        modifier,
                        Map.of("artefact.add", artefact.sourceUrl()),
                        context);
                dataStore.commit();
                return caseObject;
            } catch (Throwable t) {
                dataStore.rollback(false);
                throw t;
            }
        } catch (CaseAuthorizationError | CaseActionException ae) {
            throw ae;
        } catch (Throwable ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseFatalError(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public Case<P, S, E, T> removeArtefact(@NonNull String caseId,
                                           @NonNull String artefactId,
                                           @NonNull UserOrRole modifier,
                                           Context context) throws CaseAuthorizationError, CaseActionException {
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            context = AbstractDataStore.withRefresh(context);
            Case<P, S, E, T> caseObject = findById(id,
                    (Class<? extends Case<P, S, E, T>>) settings.getCaseType(),
                    true,
                    modifier,
                    context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
            authorization.authorize(caseObject, EStandardAction.DeleteArtefact.action(), modifier, context);
            DocumentId docId = new DocumentId(settings.getContentCollection(), artefactId);
            if (!caseObject.deleteArtefact(docId)) {
                throw new Exception(String.format("Artefact not found. [case id=%s][document id=%s]",
                        caseId, artefactId));
            }
            dataStore.beingTransaction();
            try {
                caseObject = saveUpdate(caseObject,
                        EStandardAction.DeleteArtefact.action(),
                        EStandardCode.ActionRemoveArtefact.code(),
                        null,
                        modifier,
                        Map.of("artefact.removed", artefactId),
                        context);
                dataStore.commit();
                return caseObject;
            } catch (Throwable t) {
                dataStore.rollback(false);
                throw t;
            }
        } catch (CaseAuthorizationError | CaseActionException ae) {
            throw ae;
        } catch (Throwable ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseFatalError(ex);
        }
    }

    @SuppressWarnings("unchecked")
    protected Case<P, S, E, T> updateArtefact(@NonNull String caseId,
                                              @NonNull CaseDocument<E, T> document,
                                              @NonNull UserOrRole modifier,
                                              Context context) throws Exception {
        checkState();
        CaseId id = new CaseId(caseId);
        context = AbstractDataStore.withRefresh(context);
        Case<P, S, E, T> caseObject = findById(id,
                (Class<? extends Case<P, S, E, T>>) settings.getCaseType(),
                true,
                modifier,
                context);
        if (caseObject == null) {
            throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                    caseId, settings.getCaseType().getCanonicalName()));
        }
        authorization.authorize(caseObject, EStandardAction.UpdateArtefact.action(), modifier, context);
        DocumentContext docCtx = null;
        if (context != null) {
            docCtx = new DocumentContext(context);
        } else {
            docCtx = new DocumentContext();
        }
        docCtx.user(modifier.asPrincipal());
        dataStore.beingTransaction();
        try {
            document = (CaseDocument<E, T>) contentProvider.update(document, docCtx);
            addCaseHistory(caseObject.getId(),
                    EStandardAction.UpdateArtefact.action(),
                    EStandardCode.ActionUpdateArtefactState.code(),
                    null,
                    document,
                    modifier);
            dataStore.commit();
            return caseObject;
        } catch (Throwable t) {
            dataStore.rollback(false);
            throw t;
        }
    }

    @SuppressWarnings("unchecked")
    public Case<P, S, E, T> updateArtefactState(@NonNull String caseId,
                                                @NonNull String artefactId,
                                                @NonNull E state,
                                                @NonNull UserOrRole modifier,
                                                Context context) throws CaseActionException, CaseAuthorizationError {
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            context = AbstractDataStore.withRefresh(context);
            Case<P, S, E, T> caseObject = findById(id,
                    (Class<? extends Case<P, S, E, T>>) settings.getCaseType(),
                    true,
                    modifier,
                    context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
            authorization.authorize(caseObject, EStandardAction.UpdateArtefact.action(), modifier, context);
            DocumentId docId = new DocumentId(settings.getContentCollection(), artefactId);
            CaseDocument<E, T> document = caseObject.findArtefact(docId);
            if (document == null) {
                throw new Exception(String.format("Artefact not found. [case id=%s][document id=%s]",
                        caseId, artefactId));
            }
            document.setDocState(state);
            DocumentContext docCtx = null;
            if (context != null) {
                docCtx = new DocumentContext(context);
            } else {
                docCtx = new DocumentContext();
            }
            docCtx.user(modifier.asPrincipal());
            dataStore.beingTransaction();
            try {
                document = (CaseDocument<E, T>) contentProvider.update(document, docCtx);
                addCaseHistory(caseObject.getId(),
                        EStandardAction.UpdateArtefact.action(),
                        EStandardCode.ActionUpdateArtefactState.code(),
                        null,
                        document,
                        modifier);
                dataStore.commit();
                return caseObject;
            } catch (Throwable t) {
                dataStore.rollback(false);
                throw t;
            }
        } catch (CaseAuthorizationError | CaseActionException ae) {
            throw ae;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseFatalError(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public CaseComment comment(@NonNull String caseId,
                               @NonNull String comment,
                               @NonNull CaseCode reason,
                               @NonNull UserOrRole commentBy,
                               Context context) throws CaseAuthorizationError, CaseActionException {
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            Case<P, S, E, T> caseObject = (Case<P, S, E, T>) dataStore.find(caseId, settings.getCaseType(), context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
            authorization.authorize(caseObject, EStandardAction.Comment.action(), commentBy, context);
            CaseComment c = caseObject.addComment(commentBy, comment, reason, null, null, null);
            dataStore.beingTransaction();
            try {
                caseObject = saveUpdate(caseObject,
                        EStandardAction.Comment.action(),
                        reason,
                        comment,
                        commentBy,
                        Map.of("reason", reason, "comment", comment),
                        context);
                dataStore.commit();
                return c;
            } catch (Throwable t) {
                dataStore.rollback(false);
                throw t;
            }
        } catch (CaseAuthorizationError | CaseActionException ae) {
            throw ae;
        } catch (Throwable ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseFatalError(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public CaseComment commentOn(@NonNull String caseId,
                                 @NonNull String documentId,
                                 @NonNull String comment,
                                 @NonNull CaseCode reason,
                                 @NonNull UserOrRole commentBy,
                                 Context context) throws CaseAuthorizationError, CaseActionException {
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            Case<P, S, E, T> caseObject = (Case<P, S, E, T>) dataStore.find(caseId, settings.getCaseType(), context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
            authorization.authorize(caseObject, EStandardAction.Comment.action(), commentBy, context);
            DocumentId docId = new DocumentId(settings.getContentCollection(), documentId);
            CaseComment c = caseObject.addComment(commentBy, comment, reason, null, null, docId);
            dataStore.beingTransaction();
            try {
                caseObject = saveUpdate(caseObject,
                        EStandardAction.Comment.action(),
                        reason,
                        comment,
                        commentBy,
                        Map.of("reason", reason, "comment", comment),
                        context);
                dataStore.commit();
                return c;
            } catch (Throwable t) {
                dataStore.rollback(false);
                throw t;
            }
        } catch (CaseAuthorizationError | CaseActionException ae) {
            throw ae;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseFatalError(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public CaseComment respondTo(@NonNull String caseId,
                                 @NonNull Long commentId,
                                 @NonNull String comment,
                                 @NonNull CaseCode reason,
                                 @NonNull ECommentState responseState,
                                 @NonNull UserOrRole commentBy,
                                 Context context) throws CaseAuthorizationError, CaseActionException {
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            Case<P, S, E, T> caseObject = (Case<P, S, E, T>) dataStore.find(caseId, settings.getCaseType(), context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
            if (responseState != ECommentState.Closed)
                authorization.authorize(caseObject, EStandardAction.CommentRespond.action(), commentBy, context);
            else
                authorization.authorize(caseObject, EStandardAction.CommentClose.action(), commentBy, context);
            CaseComment c = caseObject.addComment(commentBy, comment, reason, commentId, responseState, null);
            dataStore.beingTransaction();
            try {
                caseObject = saveUpdate(caseObject,
                        EStandardAction.Comment.action(),
                        reason,
                        comment,
                        commentBy,
                        Map.of("response", responseState, "comment", comment),
                        context);
                dataStore.commit();
                return c;
            } catch (Throwable t) {
                dataStore.rollback(false);
                throw t;
            }
        } catch (CaseAuthorizationError | CaseActionException ae) {
            throw ae;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseFatalError(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public Case<P, S, E, T> updateCaseState(@NonNull CaseId id,
                                            @NonNull Class<? extends Case<P, S, E, T>> entityType,
                                            @NonNull P state,
                                            @NonNull String notes,
                                            @NonNull UserOrRole modifier,
                                            Context context) throws CaseActionException, CaseAuthorizationError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(notes));
        checkState();
        try {
            context = AbstractDataStore.withRefresh(context);
            Case<P, S, E, T> caseObject = findById(id,
                    (Class<? extends Case<P, S, E, T>>) settings.getCaseType(),
                    true,
                    modifier,
                    context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        id.stringKey(), entityType.getCanonicalName()));
            }
            if (caseObject.getCaseState().getState() == state) {
                throw new Exception(String.format("Case already in state. [id=%s][state=%s]",
                        id.stringKey(), state.name()));
            }
            authorization.authorize(caseObject, EStandardAction.UpdateState.action(), modifier, context);
            P current = caseObject.getCaseState().getState();
            caseObject.getCaseState().setState(state);
            if (context instanceof CaseContext ctx) {
                if (((CaseContext) context).customFields() != null) {
                    authorization.authorize(caseObject, EStandardAction.Update.action(), modifier, context);
                    Map<String, Object> values = ctx.customFields();
                    for (String field : values.keySet()) {
                        Object v = values.get(field);
                        BeanUtils.setValue(caseObject, field, v);
                    }
                }
            }
            Map<String, Object> diff = new HashMap<>();
            diff.put("state", state);
            if (context instanceof CaseContext) {
                diff.put("updates", context);
            }
            dataStore.beingTransaction();
            try {
                transition(current, caseObject.getCaseState().getState(), caseObject);
                caseObject = saveUpdate(caseObject,
                        EStandardAction.UpdateState.action(),
                        EStandardCode.UpdateSate.code(),
                        notes,
                        modifier,
                        diff,
                        context);
                dataStore.commit();
            } catch (Throwable t) {
                dataStore.rollback(false);
                throw t;
            }
            return caseObject;
        } catch (CaseAuthorizationError | CaseActionException ae) {
            throw ae;
        } catch (Throwable ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseFatalError(ex);
        }
    }

    private void transition(P fromState, P toState, Case<P, S, E, T> caseObject) throws CaseActionException {
        if (transitions != null) {
            if (transitions.containsKey(fromState)) {
                Map<P, StateTransitionHandler<P, S, E, T>> map = transitions.get(fromState);
                if (map.containsKey(toState)) {
                    StateTransitionHandler<P, S, E, T> handler = map.get(toState);
                    try {
                        handler.handleStateTransition(fromState, caseObject);
                    } catch (CaseActionException | CaseFatalError ae) {
                        throw ae;
                    } catch (Throwable t) {
                        throw new CaseFatalError(t);
                    }
                }
            }
        }
        handleStateTransition(fromState, caseObject);
    }

    @SuppressWarnings("unchecked")
    protected Case<P, S, E, T> update(@NonNull Case<P, S, E, T> caseObject,
                                      @NonNull CaseAction action,
                                      @NonNull CaseCode code,
                                      @NonNull String notes,
                                      @NonNull UserOrRole modifier,
                                      Context context) throws CaseActionException, CaseAuthorizationError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(notes));
        checkState();
        try {
            authorization.authorize(caseObject, EStandardAction.Update.action(), modifier, context);
            dataStore.beingTransaction();
            try {
                caseObject = saveUpdate(caseObject,
                        action,
                        code,
                        notes,
                        modifier,
                        Map.of("action", action, "code", code, "notes", notes),
                        context);
                dataStore.commit();
                return caseObject;
            } catch (Throwable t) {
                dataStore.rollback(false);
                throw t;
            }
        } catch (CaseAuthorizationError | CaseActionException ae) {
            throw ae;
        } catch (Throwable ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseFatalError(ex);
        }
    }

    private Case<P, S, E, T> saveUpdate(Case<P, S, E, T> caseObject,
                                        @NonNull CaseAction action,
                                        @NonNull CaseCode code,
                                        String notes,
                                        UserOrRole modifier,
                                        Object diff,
                                        Context context) throws Exception {
        // context = AbstractDataStore.withRefresh(context);
        caseObject = save(caseObject, modifier, context);
        addCaseHistory(caseObject.getId(),
                action,
                code,
                notes,
                diff,
                modifier);
        return caseObject;
    }

    public Case<P, S, E, T> findById(@NonNull CaseId id,
                                     @NonNull Class<? extends Case<P, S, E, T>> entityType,
                                     boolean fetchDocuments,
                                     @NonNull UserOrRole caller,
                                     Context context) throws CaseAuthorizationError, CaseActionException {
        try {
            authorization.authorizeRead(EStandardAction.Read.action(),
                    caller,
                    context);
            Case<P, S, E, T> caseObject = dataStore.find(id, entityType, context);
            if (caseObject != null) {
                if (fetchDocuments) {
                    fetchDocuments(caseObject);
                }
            }
            return caseObject;
        } catch (CaseAuthorizationError | CaseActionException e) {
            throw e;
        } catch (Throwable t) {
            throw new CaseActionException(t);
        }
    }

    public <C extends Case<P, S, E, T>> List<C> search(@NonNull AbstractDataStore.Q query,
                                                       @NonNull Class<C> entityType,
                                                       int currentPage,
                                                       int batchSize,
                                                       boolean fetchDocuments,
                                                       @NonNull UserOrRole caller,
                                                       Context context) throws CaseAuthorizationError, CaseActionException {
        try {
            authorization.authorizeRead(EStandardAction.Read.action(),
                    caller,
                    context);
            try (Cursor<CaseId, Case<P, S, E, T>> cursor = dataStore()
                    .search(query, currentPage, batchSize, CaseId.class, entityType, context)) {
                if (cursor != null) {
                    List<C> cases = new ArrayList<>();
                    while (true) {
                        List<Case<P, S, E, T>> result = cursor.nextPage();
                        if (result == null || result.isEmpty()) break;
                        for (Case<P, S, E, T> c : result) {
                            if (c.getCaseState().getState() != c.getCaseState().getDeletedState()) {
                                if (fetchDocuments) {
                                    fetchDocuments(c);
                                }
                                cases.add((C) c);
                            }
                        }
                    }
                    if (!cases.isEmpty()) {
                        return cases;
                    }
                }
            }
        } catch (CaseAuthorizationError | CaseActionException e) {
            throw e;
        } catch (Exception ex) {
            throw new CaseActionException(ex);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <C extends Case<P, S, E, T>> List<C> findByName(@NonNull String caseName,
                                                           @NonNull Class<C> entityType,
                                                           boolean fetchDocuments,
                                                           @NonNull UserOrRole caller,
                                                           Context context) throws CaseAuthorizationError, CaseActionException {
        String condition = "name = :name";
        Map<String, Object> params = Map.of("name", caseName);
        AbstractDataStore.Q query = new AbstractDataStore.Q()
                .where(condition)
                .addAll(params);
        try {
            authorization.authorizeRead(EStandardAction.Read.action(),
                    caller,
                    context);
            try (Cursor<CaseId, Case<P, S, E, T>> cursor = dataStore()
                    .search(query, CaseId.class, entityType, context)) {
                if (cursor != null) {
                    List<C> cases = new ArrayList<>();
                    while (true) {
                        List<Case<P, S, E, T>> result = cursor.nextPage();
                        if (result == null || result.isEmpty()) break;
                        for (Case<P, S, E, T> c : result) {
                            if (c.getCaseState().getState() != c.getCaseState().getDeletedState()) {
                                if (fetchDocuments) {
                                    fetchDocuments(c);
                                }
                                cases.add((C) c);
                            }
                        }
                    }
                    if (!cases.isEmpty()) {
                        return cases;
                    }
                }
            }
        } catch (CaseAuthorizationError | CaseActionException e) {
            throw e;
        } catch (Exception ex) {
            throw new CaseActionException(ex);
        }
        return null;
    }

    protected void fetchDocuments(@NonNull Case<P, S, E, T> caseObject) throws DataStoreException, CaseActionException {
        if (caseObject.getArtefactReferences() != null) {
            Set<CaseDocument<E, T>> docs = new HashSet<>();
            for (ArtefactReference ref : caseObject.getArtefactReferences()) {
                CaseDocument<E, T> doc = (CaseDocument<E, T>) contentProvider
                        .find(ref.getId().getDocumentId(), documentType, false, null);
                if (doc == null) {
                    throw new CaseActionException(String.format("[case id=%s] Referenced document not found. [doc id=%s]",
                            caseObject.getId().stringKey(), ref.getId().getDocumentId().stringKey()));
                }
                docs.add(doc);
            }
            caseObject.setArtefacts(docs);
        }
    }

    @SuppressWarnings("unchecked")
    private Case<P, S, E, T> save(Case<P, S, E, T> caseObject, UserOrRole user, Context context) throws Exception {
        DocumentContext docCtx = null;
        if (context != null) {
            docCtx = new DocumentContext(context);
        } else {
            docCtx = new DocumentContext();
        }
        docCtx.user(user.asPrincipal());
        if (caseObject.getState().getState() == EEntityState.New) {
            if (caseObject.getArtefacts() != null) {
                for (CaseDocument<E, T> document : caseObject.getArtefacts()) {
                    contentProvider.create(document, docCtx);
                }
            }
            caseObject = dataStore.create(caseObject, caseObject.getClass(), context);
        } else {
            if (caseObject.getArtefacts() != null) {
                for (CaseDocument<E, T> document : caseObject.getArtefacts()) {
                    if (document.getState().getState() == EEntityState.New)
                        contentProvider.create(document, docCtx);
                }
            }
            if (caseObject.getDeleted() != null) {
                for (CaseDocument<E, T> doc : caseObject.getDeleted()) {
                    doc.setReferenceId(null);
                    doc.getState().setState(EEntityState.Updated);
                    contentProvider.update(doc, docCtx);
                }
            }
            caseObject = dataStore.update(caseObject, caseObject.getClass(), context);
        }
        return caseObject;
    }

    @Override
    public void close() throws IOException {
        if (!state.hasError()) {
            state.setState(ProcessorState.EProcessorState.Stopped);
        }
        if (contentProvider != null) {
            contentProvider.close();
        }
        if (handlers != null) {
            for (StateTransitionHandler<?, ?, ?, ?> handler : handlers.values()) {
                handler.close();
            }
            handlers.clear();
            handlers = null;
        }
    }

    protected abstract Case<P, S, E, T> createInstance() throws Exception;

    protected abstract void validateCase(@NonNull Case<P, S, E, T> caseObject) throws ValidationExceptions;

    protected abstract @NonNull CaseDocument<E, T> validateArtefact(@NonNull CaseDocument<E, T> document) throws ValidationExceptions;

    protected abstract void validateAssignment(UserOrRole from, Case<P, S, E, T> caseObject) throws ValidationExceptions;

    protected abstract void handleStateTransition(@NonNull P previousState,
                                                  @NonNull Case<P, S, E, T> caseObject) throws CaseActionException;
}
