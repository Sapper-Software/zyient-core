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

package io.zyient.core.content;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DateTimeUtils;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.core.content.settings.ManagedProviderSettings;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.Reader;
import io.zyient.core.filesystem.Writer;
import io.zyient.core.filesystem.env.FileSystemEnv;
import io.zyient.core.filesystem.impl.PostOperationVisitor;
import io.zyient.core.filesystem.model.FileInode;
import io.zyient.core.filesystem.model.Inode;
import io.zyient.core.filesystem.model.PathInfo;
import io.zyient.core.persistence.*;
import io.zyient.core.persistence.impl.solr.SolrDataStore;
import io.zyient.core.persistence.model.Document;
import io.zyient.core.persistence.model.DocumentId;
import io.zyient.core.persistence.model.DocumentState;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.auth.BasicUserPrincipal;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class ManagedContentProvider<T> extends ContentProvider implements PostOperationVisitor {
    public static class Constants {
        public static final String ATTRIBUTE_DOC_ID = "document.ref.id";
        public static final String ATTRIBUTE_DOC_TYPE = "document.class";
    }

    private AbstractDataStore<T> dataStore;
    private FileSystem fileSystem;

    protected ManagedContentProvider(@NonNull Class<? extends ContentProviderSettings> settingsType) {
        super(settingsType);
    }

    private String getPath(DocumentId id) {
        ManagedProviderSettings settings = (ManagedProviderSettings) settings();
        String dir = DateTimeUtils.formatTimestamp(settings.getPathFormat());
        return PathUtils.formatPath(String.format("%s/%s/%s", id.getCollection(), dir, id.getId()));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doConfigure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            ManagedProviderSettings settings = (ManagedProviderSettings) settings();
            if (!(env() instanceof DataStoreProvider)) {
                throw new ConfigurationException(
                        String.format("Environment does not initialize DataStore manager. [type=%s]",
                                env().getClass().getCanonicalName()));
            }
            if (!(env() instanceof FileSystemEnv<?>)) {
                throw new ConfigurationException(
                        String.format("Environment does not provide filesystem(s). [type=%s]",
                                env().getClass().getCanonicalName()));
            }
            DataStoreManager dataStoreManager = ((DataStoreProvider) env()).getDataStoreManager();
            Preconditions.checkNotNull(dataStoreManager);
            dataStore = (AbstractDataStore<T>) dataStoreManager.getDataStore(settings.getDataStore(), settings.getDataStoreType());
            if (dataStore == null) {
                throw new ConfigurationException(String.format("DataStore not found. [name=%s][type=%s]",
                        settings.getDataStore(), SolrDataStore.class.getCanonicalName()));
            }
            fileSystem = ((FileSystemEnv<?>) env()).fileSystemManager().get(settings.getFileSystem());
            if (fileSystem == null) {
                throw new ConfigurationException(String.format("FileSystem not found. [name=%s]",
                        settings.getFileSystem()));
            }
            fileSystem.addVisitor(this);
            DefaultLogger.info(String.format("Using FileSystem [%s]: [type=%s]",
                    fileSystem.settings().getName(), fileSystem.getClass().getCanonicalName()));
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    protected <E extends DocumentState<?>, K extends IKey, D extends Document<E, K, D>> Document<E, K, D> createDoc(@NonNull Document<E, K, D> document,
                                                                                                                    @NonNull DocumentContext context) throws DataStoreException {
        try {
            FileInode fi = fileSystem.create(document.getId().getCollection(), getPath(document.getId()));
            String idJson = JSONUtils.asString(document.getId());
            fi.attribute(Constants.ATTRIBUTE_DOC_ID, idJson);
            fi.attribute(Constants.ATTRIBUTE_DOC_TYPE, document.getClass().getCanonicalName());
            fi = (FileInode) fileSystem.updateInode(fi);
            PathInfo pi = fi.getPathInfo();
            Preconditions.checkNotNull(pi);
            document.setUri(pi.uri());
            document.setProperty(Document.PROPERTY_PATH_MAP, pi.pathConfig());
            document.validate();
            if (dataStore instanceof TransactionDataStore<?, ?>) {
                ((TransactionDataStore<?, ?>) dataStore).beingTransaction();
            }
            try {
                document = dataStore.create(document, document.getClass(), context);
                if (dataStore instanceof TransactionDataStore<?, ?>) {
                    ((TransactionDataStore<?, ?>) dataStore).commit();
                }
                try (Writer writer = fileSystem.writer(fi, document.getPath())) {
                    writer.commit(true);
                }
                return document;
            } catch (RuntimeException re) {
                if (dataStore instanceof TransactionDataStore<?, ?>) {
                    ((TransactionDataStore<?, ?>) dataStore).rollback(false);
                }
                throw re;
            } catch (Throwable t) {
                if (dataStore instanceof TransactionDataStore<?, ?>) {
                    ((TransactionDataStore<?, ?>) dataStore).rollback(false);
                }
                throw t;
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <E extends DocumentState<?>, K extends IKey, D extends Document<E, K, D>> Document<E, K, D> updateDoc(@NonNull Document<E, K, D> document,
                                                                                                                    @NonNull DocumentContext context) throws DataStoreException {
        try {
            Map<String, String> map = document.pathConfig();
            PathInfo pi = fileSystem.parsePathInfo(map);
            FileInode fi = (FileInode) fileSystem.getInode(pi);
            if (fi == null) {
                throw new DataStoreException(String.format("Document not found. [uri=%s]", document.getUri()));
            }
            DocumentId docId = JSONUtils.read(fi.attribute(Constants.ATTRIBUTE_DOC_ID), DocumentId.class);
            if (docId.compareTo(document.getId()) != 0) {
                throw new DataStoreException(String.format("Document mis-match: [FS ID=%s][DOC ID=%s]",
                        docId.stringKey(), document.getId().stringKey()));
            }
            document.validate();
            if (dataStore instanceof TransactionDataStore<?, ?>) {
                ((TransactionDataStore<?, ?>) dataStore).beingTransaction();
            }
            try {
                document = dataStore.update(document, document.getClass(), context);
                if (dataStore instanceof TransactionDataStore<?, ?>) {
                    ((TransactionDataStore<?, ?>) dataStore).commit();
                }
                try (Writer writer = fileSystem.writer(fi, document.getPath())) {
                    writer.commit(true);
                }
                return document;
            } catch (RuntimeException re) {
                if (dataStore instanceof TransactionDataStore<?, ?>) {
                    ((TransactionDataStore<?, ?>) dataStore).rollback(false);
                }
                throw re;
            } catch (Throwable t) {
                if (dataStore instanceof TransactionDataStore<?, ?>) {
                    ((TransactionDataStore<?, ?>) dataStore).rollback(false);
                }
                throw t;
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <E extends DocumentState<?>, K extends IKey, D extends Document<E, K, D>> boolean deleteDoc(@NonNull DocumentId id,
                                                                                                          @NonNull Class<? extends Document<E, K, D>> entityType,
                                                                                                          @NonNull DocumentContext context) throws DataStoreException {
        try {
            Document<?, ?, ?> doc = findDoc(id, entityType, false, context);
            if (doc != null) {
                Map<String, String> map = doc.pathConfig();
                PathInfo pi = fileSystem.parsePathInfo(map);
                FileInode fi = (FileInode) fileSystem.getInode(pi);
                if (fi == null) {
                    DefaultLogger.warn(String.format("Document not found. [uri=%s]", doc.getUri()));
                } else {
                    DocumentId docId = JSONUtils.read(fi.attribute(Constants.ATTRIBUTE_DOC_ID), DocumentId.class);
                    if (docId.compareTo(id) != 0) {
                        throw new DataStoreException(String.format("Document mis-match: [FS ID=%s][DOC ID=%s]",
                                docId.stringKey(), id.stringKey()));
                    }
                    if (!fileSystem.delete(fi.getPathInfo())) {
                        DefaultLogger.warn(String.format("Document delete returned false. [uri=%s]", doc.getUri()));
                    }
                }
                if (dataStore instanceof TransactionDataStore<?, ?>) {
                    ((TransactionDataStore<?, ?>) dataStore).beingTransaction();
                }
                try {
                    boolean r = dataStore.delete(id, Document.class, context);
                    if (dataStore instanceof TransactionDataStore<?, ?>) {
                        ((TransactionDataStore<?, ?>) dataStore).commit();
                    }
                    return r;
                } catch (RuntimeException re) {
                    if (dataStore instanceof TransactionDataStore<?, ?>) {
                        ((TransactionDataStore<?, ?>) dataStore).rollback(false);
                    }
                    throw re;
                } catch (Throwable t) {
                    if (dataStore instanceof TransactionDataStore<?, ?>) {
                        ((TransactionDataStore<?, ?>) dataStore).rollback(false);
                    }
                    throw t;
                }
            }
            return false;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    protected <E extends DocumentState<?>, K extends IKey, D extends Document<E, K, D>> Map<DocumentId, Boolean> deleteDocs(@NonNull List<DocumentId> ids,
                                                                                                                            @NonNull Class<? extends Document<E, K, D>> entityType,
                                                                                                                            @NonNull DocumentContext context) throws DataStoreException {
        try {
            Map<DocumentId, Boolean> response = new HashMap<>();
            for (DocumentId id : ids) {
                boolean r = deleteDoc(id, entityType, context);
                response.put(id, r);
            }
            return response;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <E extends DocumentState<?>, K extends IKey, D extends Document<E, K, D>> Document<E, K, D> findDoc(@NonNull DocumentId docId,
                                                                                                                  @NonNull Class<? extends Document<E, K, D>> entityType,
                                                                                                                  boolean download,
                                                                                                                  DocumentContext context) throws DataStoreException {
        try {
            Document<E, K, D> doc = dataStore.find(docId, entityType, context);
            if (doc != null) {
                if (download) {
                    Map<String, String> map = doc.pathConfig();
                    PathInfo pi = fileSystem.parsePathInfo(map);
                    FileInode fi = (FileInode) fileSystem.getInode(pi);
                    if (fi == null) {
                        throw new DataStoreException(String.format("Document not found. [uri=%s]", doc.getUri()));
                    }
                    try (Reader reader = fileSystem.reader(pi)) {
                        File path = reader.copy();
                        doc.setPath(path);
                    }
                }
                return doc;
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    protected <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> Cursor<DocumentId, Document<E, K, T>> findChildDocs(@NonNull DocumentId docId,
                                                                                                                                            @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                                                            DocumentContext context) throws DataStoreException {
        String q = String.format("parentDocId = '%s'", docId.getId());
        AbstractDataStore.Q query = new AbstractDataStore.Q()
                .where(q);
        return search(query, entityType, context);
    }

    protected abstract <E extends DocumentState<?>, K extends IKey, D extends Document<E, K, D>> Document<E, K, D> findDoc(@NonNull String uri,
                                                                                                                           @NonNull String collection,
                                                                                                                           @NonNull Class<? extends Document<E, K, D>> entityType,
                                                                                                                           DocumentContext context) throws DataStoreException;

    @Override
    @SuppressWarnings("unchecked")
    public void visit(@NonNull Operation op, @NonNull OperationState state, Inode inode, Throwable error) {
        try {
            if (op != Operation.Upload) return;
            if (inode instanceof FileInode fi) {
                DocumentId docId = JSONUtils.read(fi.attribute(Constants.ATTRIBUTE_DOC_ID), DocumentId.class);
                Class<? extends Document<?, ?, ?>> type =
                        (Class<? extends Document<?, ?, ?>>) Class.forName(fi.attribute(Constants.ATTRIBUTE_DOC_TYPE));
                if (dataStore instanceof TransactionDataStore<?, ?>) {
                    ((TransactionDataStore<?, ?>) dataStore).beingTransaction();
                }
                try {
                    Document<?, ?, ?> document = dataStore.find(docId, type, null);
                    if (document == null) {
                        throw new Exception(String.format("Document entity not found. [id=%s]", docId.stringKey()));
                    }
                    if (state == OperationState.Error) {
                        document.getDocState().error(error);
                    } else {
                        document.getDocState().available();
                    }
                    document = dataStore.update(document, document.getClass(), new InternalUserContext(document));
                    if (dataStore instanceof TransactionDataStore<?, ?>) {
                        ((TransactionDataStore<?, ?>) dataStore).commit();
                    }
                } catch (RuntimeException re) {
                    if (dataStore instanceof TransactionDataStore<?, ?>) {
                        ((TransactionDataStore<?, ?>) dataStore).rollback(false);
                    }
                    throw re;
                } catch (Throwable t) {
                    if (dataStore instanceof TransactionDataStore<?, ?>) {
                        ((TransactionDataStore<?, ?>) dataStore).rollback(false);
                    }
                    throw t;
                }
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error("Visitor callback error", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

    public static class InternalUserContext extends DocumentContext {
        public InternalUserContext(@NonNull Document<?, ?, ?> document) {
            user(new BasicUserPrincipal(document.getModifiedBy()));
        }
    }
}
