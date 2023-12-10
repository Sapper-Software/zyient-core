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
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.caseflow.errors.CaseActionException;
import io.zyient.core.caseflow.errors.CaseAuthorizationError;
import io.zyient.core.caseflow.model.*;
import io.zyient.core.content.ContentProvider;
import io.zyient.core.content.DocumentContext;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.DataStoreManager;
import io.zyient.core.persistence.EConnectionState;
import io.zyient.core.persistence.env.DataStoreEnv;
import io.zyient.core.persistence.model.DocumentId;
import io.zyient.core.persistence.model.DocumentState;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class CaseManager<S extends CaseState<?>, E extends DocumentState<?>, T extends CaseDocument<E, T>> {
    private final ProcessorState state = new ProcessorState();
    private final Class<? extends CaseManagerSettings> settingsType;

    private AbstractDataStore<?> dataStore;
    private ContentProvider contentProvider;
    private CaseManagerSettings settings;
    private ActionAuthorization<S> authorization;
    private DataStoreEnv<?> env;

    public CaseManager(@NonNull Class<? extends CaseManagerSettings> settingsType) {
        this.settingsType = settingsType;
    }

    @SuppressWarnings("unchecked")
    public CaseManager<S, E, T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
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
            dataStore = dataStoreManager.getDataStore(settings.getDataStore(),
                    settings.getDataStoreType());
            if (dataStore == null) {
                throw new Exception(String.format("Data Store not found. [name=%s][type=%s]",
                        settings.getDataStore(), settings.getDataStoreType().getCanonicalName()));
            }
            HierarchicalConfiguration<ImmutableNode> contentConfig = reader.config()
                    .configurationAt(ContentProvider.__CONFIG_PATH);
            Class<? extends ContentProvider> clazz
                    = (Class<? extends ContentProvider>) ConfigReader.readType(reader.config());
            contentProvider = clazz.getDeclaredConstructor()
                    .newInstance()
                    .configure(reader.config(), env);
            authorization = (ActionAuthorization<S>) settings.getAuthorizer()
                    .getDeclaredConstructor()
                    .newInstance();
            authorization.configure(reader.config());
            state.setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    protected void checkState() throws CaseActionException {
        if (!state.isRunning()) {
            throw new CaseActionException(String.format("CaseManager: Invalid state. [state=%s]",
                    state.getState().name()));
        }
    }

    @SuppressWarnings("unchecked")
    public Case<S, E, T> create(@NonNull String description,
                                List<Artefact> artefacts,
                                @NonNull UserOrRole creator,
                                Context context) throws CaseActionException,
            CaseAuthorizationError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(description));
        checkState();
        try {
            authorization.authorize(null, ECaseAction.Create, creator, context);
            Case<S, E, T> caseObject = createInstance();
            if (caseObject.getCaseState() == null) {
                throw new Exception(String.format("Invalid case instance: state is null. [type=%s]",
                        caseObject.getClass().getCanonicalName()));
            }
            caseObject.getState().setState(EEntityState.New);
            if (caseObject.getId() == null) {
                caseObject.setId(new CaseId());
            }
            caseObject.setCreatedBy(new Actor(creator));
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

            return save(caseObject, creator, context);
        } catch (CaseAuthorizationError ae) {
            throw ae;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseActionException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public Case<S, E, T> assignTo(@NonNull String caseId,
                                  UserOrRole assignTo,
                                  @NonNull UserOrRole assigner,
                                  Context context) throws CaseActionException, CaseAuthorizationError {
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            Case<S, E, T> caseObject = (Case<S, E, T>) dataStore.find(caseId, settings.getCaseType(), context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
            Actor current = caseObject.getAssignedTo();
            if (assignTo != null) {
                authorization.authorize(caseObject, ECaseAction.AssignTo, assigner, context);
                authorization.checkAssignment(caseObject, assignTo, context);
            } else
                authorization.authorize(caseObject, ECaseAction.RemoveAssignment, assigner, context);
            if (assignTo != null)
                caseObject.setAssignedTo(new Actor(assignTo));
            else
                caseObject.setAssignedTo(null);
            validateAssignment(current, caseObject);
            caseObject.getState().setState(EEntityState.Updated);
            return save(caseObject, assigner, context);
        } catch (CaseAuthorizationError ae) {
            throw ae;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseActionException(ex);
        }
    }


    @SuppressWarnings("unchecked")
    public Case<S, E, T> addArtefact(@NonNull String caseId,
                                     @NonNull Artefact artefact,
                                     @NonNull UserOrRole modifier,
                                     Context context) throws CaseAuthorizationError, CaseActionException {
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            Case<S, E, T> caseObject = (Case<S, E, T>) dataStore.find(caseId, settings.getCaseType(), context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
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
            document.getState().setState(EEntityState.New);
            if (!Strings.isNullOrEmpty(artefact.mimeType())) {
                document.setMimeType(artefact.mimeType());
            }
            if (!Strings.isNullOrEmpty(artefact.password())) {
                document.setPassword(artefact.password());
            }
            document = validateArtefact(document);
            caseObject.addArtefact(document);
            return save(caseObject, modifier, context);
        } catch (CaseAuthorizationError ae) {
            throw ae;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseActionException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public Case<S, E, T> removeArtefact(@NonNull String caseId,
                                        @NonNull String artefactId,
                                        @NonNull UserOrRole modifier,
                                        Context context) throws CaseAuthorizationError, CaseActionException {
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            Case<S, E, T> caseObject = (Case<S, E, T>) dataStore.find(caseId, settings.getCaseType(), context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
            authorization.authorize(caseObject, ECaseAction.DeleteArtefact, modifier, context);
            DocumentId docId = new DocumentId(settings.getContentCollection(), artefactId);
            if (!caseObject.deleteArtefact(docId)) {
                throw new Exception(String.format("Artefact not found. [case id=%s][document id=%s]",
                        caseId, artefactId));
            }
            return save(caseObject, modifier, context);
        } catch (CaseAuthorizationError ae) {
            throw ae;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseActionException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public CaseDocument<E, T> updateArtefactState(@NonNull String caseId,
                                                  @NonNull String artefactId,
                                                  @NonNull E state,
                                                  @NonNull UserOrRole modifier,
                                                  Context context) throws CaseActionException, CaseAuthorizationError {
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            Case<S, E, T> caseObject = (Case<S, E, T>) dataStore.find(caseId, settings.getCaseType(), context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
            authorization.authorize(caseObject, ECaseAction.UpdateArtefact, modifier, context);
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
            return (CaseDocument<E, T>) contentProvider.update(document, docCtx);
        } catch (CaseAuthorizationError ae) {
            throw ae;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseActionException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public CaseComment comment(@NonNull String caseId,
                               @NonNull String comment,
                               @NonNull UserOrRole commentBy,
                               Context context) throws CaseAuthorizationError, CaseActionException {
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            Case<S, E, T> caseObject = (Case<S, E, T>) dataStore.find(caseId, settings.getCaseType(), context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
            authorization.authorize(caseObject, ECaseAction.Comment, commentBy, context);
            CaseComment c = caseObject.addComment(commentBy, comment, null, null, null);
            save(caseObject, commentBy, context);
            return c;
        } catch (CaseAuthorizationError ae) {
            throw ae;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseActionException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public CaseComment commentOn(@NonNull String caseId,
                                 @NonNull String documentId,
                                 @NonNull String comment,
                                 @NonNull UserOrRole commentBy,
                                 Context context) throws CaseAuthorizationError, CaseActionException {
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            Case<S, E, T> caseObject = (Case<S, E, T>) dataStore.find(caseId, settings.getCaseType(), context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
            authorization.authorize(caseObject, ECaseAction.Comment, commentBy, context);
            DocumentId docId = new DocumentId(settings.getContentCollection(), documentId);
            CaseComment c = caseObject.addComment(commentBy, comment, null, null, docId);
            save(caseObject, commentBy, context);
            return c;
        } catch (CaseAuthorizationError ae) {
            throw ae;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseActionException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public CaseComment respondTo(@NonNull String caseId,
                                 @NonNull Long commentId,
                                 @NonNull String comment,
                                 @NonNull ECaseCommentState responseState,
                                 @NonNull UserOrRole commentBy,
                                 Context context) throws CaseAuthorizationError, CaseActionException {
        checkState();
        try {
            CaseId id = new CaseId(caseId);
            Case<S, E, T> caseObject = (Case<S, E, T>) dataStore.find(caseId, settings.getCaseType(), context);
            if (caseObject == null) {
                throw new Exception(String.format("Case not found. [id=%s][type=%s]",
                        caseId, settings.getCaseType().getCanonicalName()));
            }
            authorization.authorize(caseObject, ECaseAction.Comment, commentBy, context);
            CaseComment c = caseObject.addComment(commentBy, comment, commentId, responseState, null);
            save(caseObject, commentBy, context);
            return c;
        } catch (CaseAuthorizationError ae) {
            throw ae;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new CaseActionException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private Case<S, E, T> save(Case<S, E, T> caseObject, UserOrRole user, Context context) throws Exception {
        DocumentContext docCtx = null;
        if (context != null) {
            docCtx = new DocumentContext(context);
        } else {
            docCtx = new DocumentContext();
        }
        docCtx.user(user.asPrincipal());
        if (caseObject.getState().getState() == EEntityState.New) {
            caseObject = dataStore.create(caseObject, caseObject.getClass(), context);
            if (caseObject.getArtefacts() != null) {
                for (CaseDocument<E, T> document : caseObject.getArtefacts()) {
                    contentProvider.create(document, docCtx);
                }
            }
        } else {
            caseObject = dataStore.update(caseObject, caseObject.getClass(), context);
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
        }
        return caseObject;
    }

    protected abstract Case<S, E, T> createInstance() throws Exception;

    protected abstract void validateCase(@NonNull Case<S, E, T> caseObject) throws Exception;

    protected abstract @NonNull CaseDocument<E, T> validateArtefact(@NonNull CaseDocument<E, T> document) throws Exception;

    protected abstract void validateAssignment(UserOrRole from, Case<S, E, T> caseObject) throws Exception;
}