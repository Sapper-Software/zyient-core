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
import com.google.common.base.Strings;
import io.zyient.base.common.StateException;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.*;
import io.zyient.base.common.utils.beans.BeanUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.keystore.KeyStore;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.utils.FileTypeDetector;
import io.zyient.base.core.utils.SourceTypes;
import io.zyient.base.core.utils.Timer;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.Cursor;
import io.zyient.core.persistence.DataStoreException;
import io.zyient.core.persistence.model.Document;
import io.zyient.core.persistence.model.DocumentId;
import io.zyient.core.persistence.model.DocumentState;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import javax.crypto.SecretKey;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public abstract class ContentProvider implements Closeable {
    public static final String ENCRYPTED_PREFIX = "encrypted";
    public static final String ENCRYPTED_REGEX = String.format("%s=\\[(.+)\\]", ENCRYPTED_PREFIX);
    public static final String KEY_ENGINE = "ContentProvider";
    public static final String __CONFIG_PATH = "content-manager";
    public static final String CONTENT_TEMP_PATH = "content/temp";

    private static final Pattern ENCRYPTED = Pattern.compile(ENCRYPTED_REGEX);
    private final ProcessorState state = new ProcessorState();
    private final Class<? extends ContentProviderSettings> settingsType;

    private ContentProviderSettings settings;
    private File baseDir;
    private BaseEnv<?> env;
    private ContentProviderMetrics metrics;
    private SecretKey secretKey;

    protected ContentProvider(@NonNull Class<? extends ContentProviderSettings> settingsType) {
        this.settingsType = settingsType;
    }

    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    public ContentProvider configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                     @NonNull BaseEnv<?> env) throws ConfigurationException {
        synchronized (this) {
            this.env = env;
            try {
                ConfigReader reader = new ConfigReader(xmlConfig, __CONFIG_PATH, settingsType);
                reader.read();
                settings = (ContentProviderSettings) reader.settings();
                settings.validate();

                baseDir = new File(settings.getBaseDir());
                if (!baseDir.exists()) {
                    if (!baseDir.mkdirs()) {
                        throw new IOException(String.format("Failed to create local directory. [path=%s]",
                                baseDir.getAbsolutePath()));
                    }
                }
                if (!Strings.isNullOrEmpty(settings.getEncryptionKey())) {
                    KeyStore keyStore = env.keyStore();
                    if (keyStore == null) {
                        throw new Exception(String.format("KeyStore not specified. [env=%s]", env.name()));
                    }
                    String value = keyStore.read(settings.getEncryptionKey());
                    if (Strings.isNullOrEmpty(value)) {
                        throw new Exception(String.format("Encryption key not found. [key=%s]",
                                settings.getEncryptionKey()));
                    }
                    value = CypherUtils.checkPassword(value, ENCRYPTED_PREFIX);
                    secretKey = keyStore.generate(value, KeyStore.CIPHER_TYPE);
                }
                doConfigure(reader.config());
                state.setState(ProcessorState.EProcessorState.Running);
                metrics = new ContentProviderMetrics(KEY_ENGINE,
                        settings.getName(), getClass().getSimpleName(), env, getClass());
                return this;
            } catch (Exception ex) {
                state.error(ex);
                DefaultLogger.stacktrace(ex);
                throw new ConfigurationException(ex);
            }
        }
    }

    protected void checkState(@NonNull ProcessorState.EProcessorState state) throws StateException {
        if (this.state.getState() != state) {
            throw new StateException(String.format("Invalid state: [expected=%s][current=%s]",
                    state.name(), this.state.getState().name()));
        }
    }

    @SuppressWarnings("unchecked")
    public <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> Document<E, K, T> create(@NonNull Document<E, K, T> document,
                                                                                                              @NonNull DocumentContext context) throws DataStoreException {
        try {
            checkState(ProcessorState.EProcessorState.Running);
            if (document.getPath() == null) {
                throw new DataStoreException(String.format("Document local path missing. [doc id=%s]",
                        document.getId().stringKey()));
            }
            if (!document.getPath().exists()) {
                throw new DataStoreException(String.format("Local file not found. [doc id=%s][path=%s]",
                        document.getId().stringKey(), document.getPath().getAbsolutePath()));
            }
            metrics.createCounter().increment();
            try (Timer t = new Timer(metrics.createTimer())) {
                if (Strings.isNullOrEmpty(document.getName())) {
                    String name = document.getPath().getName();
                    document.setName(name);
                }
                SourceTypes type = SourceTypes.UNKNOWN;
                if (Strings.isNullOrEmpty(document.getMimeType())) {
                    FileTypeDetector detector = new FileTypeDetector(document.getPath());
                    type = detector.detect();
                    if (type != null) {
                        document.setMimeType(type.name());
                    }
                }
                if (type == SourceTypes.COMPRESSED && context.unpack()) {
                    if (settings.isUncompress()) {
                        uncompress(document);
                    }
                }
                document.setCreatedBy(context.user().getName());
                document.setModifiedBy(context.user().getName());
                document.setCreatedTime(System.nanoTime());
                document.setUpdatedTime(System.nanoTime());
                checkPassword(document);
                if (context.customFields() != null) {
                    Map<String, Object> values = context.customFields();
                    for (String field : values.keySet()) {
                        Object v = values.get(field);
                        BeanUtils.setValue(document, field, v);
                    }
                }
                document = createDoc(document, context);
                if (document.getDocuments() != null && !document.getDocuments().isEmpty()) {
                    Set<Document<E, K, T>> children = (Set<Document<E, K, T>>) document.getDocuments();
                    for (Document<E, K, T> child : children) {
                        child.setParentDocId(document.getId().getId());
                        child.getId().setCollection(document.getId().getCollection());

                        create(child, context);
                    }
                }
            }
            return document;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    private <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> void uncompress(Document<E, K, T> document) throws Exception {
        File dir = PathUtils.getTempDir(String.format("%s/%s", CONTENT_TEMP_PATH, document.getId().getId()));
        if (Strings.isNullOrEmpty(document.getPassword())) {
            ZipUtils.unzip(document.getPath().getAbsolutePath(), dir.getAbsolutePath());
        } else {
            String password = getPassword(document);
            ZipUtils.unzip(document.getPath().getAbsolutePath(), dir.getAbsolutePath(), password);
        }
        List<File> extracted = PathUtils.directoryList(dir, false);
        if (extracted != null && !extracted.isEmpty()) {
            for (File file : extracted) {
                Document<E, K, T> doc = document.createInstance();
                Preconditions.checkNotNull(doc.getId());
                Preconditions.checkNotNull(doc.getDocState());
                doc.setPath(file);
                doc.setName(file.getName());
                doc.setSourcePath(document.getSourcePath());
                doc.setParentDocId(document.getId().getId());
                document.add(doc);
            }
        }
    }

    private String extractIV(Document<?, ?, ?> document) {
        return document.getId().getId().substring(0, 16);
    }

    private void checkPassword(Document<?, ?, ?> document) throws Exception {
        if (Strings.isNullOrEmpty(document.getPassword())) return;
        String password = document.getPassword();
        Matcher m = ENCRYPTED.matcher(password);
        if (m.matches()) {
            return;
        }
        KeyStore keyStore = env().keyStore();
        Preconditions.checkNotNull(keyStore);
        String encrypted = CypherUtils.encryptAsString(password,
                keyStore.extractValue(secretKey, KeyStore.CIPHER_TYPE),
                extractIV(document));
        document.setPassword(String.format("%s=[%s]", ENCRYPTED_PREFIX, encrypted));
    }

    private String getPassword(Document<?, ?, ?> document) throws Exception {
        if (Strings.isNullOrEmpty(document.getPassword())) return null;
        String password = document.getPassword();
        Matcher m = ENCRYPTED.matcher(password);
        if (m.matches()) {
            String key = m.group(1);
            if (Strings.isNullOrEmpty(key)) {
                throw new Exception(String.format("Failed to extract key. [value=%s]", password));
            }
            byte[] data = CypherUtils.decrypt(key, settings.getEncryptionKey(), document.getId().stringKey());
            if (data != null && data.length > 0) {
                return new String(data, StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> Document<E, K, T> update(@NonNull Document<E, K, T> document,
                                                                                                              @NonNull DocumentContext context) throws DataStoreException {
        try {
            checkState(ProcessorState.EProcessorState.Running);
            Class<? extends Document<E, K, T>> type = (Class<? extends Document<E, K, T>>) document.getClass();
            if (document.getPath() == null) {
                throw new DataStoreException(String.format("Document local path missing. [doc id=%s]",
                        document.getId().stringKey()));
            }
            if (!document.getPath().exists()) {
                throw new DataStoreException(String.format("Local file not found. [doc id=%s][path=%s]",
                        document.getId().stringKey(), document.getPath().getAbsolutePath()));
            }
            metrics.updateCounter().increment();
            try (Timer t = new Timer(metrics.updateTimer())) {
                Set<Document<E, K, T>> children = null;
                Document<E, K, T> currrent = null;
                if (document.getDocuments() != null && !document.getDocuments().isEmpty()) {
                    children = (Set<Document<E, K, T>>) document.getDocuments();
                    currrent = find(document.getId(), type, false, context);
                    if (currrent == null) {
                        throw new Exception(String.format("Update failed: current instance not found. [id=%s]",
                                document.getId().stringKey()));
                    }
                }

                document.setModifiedBy(context.user().getName());
                document.setUpdatedTime(System.nanoTime());
                checkPassword(document);

                E state = document.getDocState();
                if (state.hasError()) state.clearError();

                document = updateDoc(document, context);
                if (currrent != null) {
                    if (currrent.getDocuments() != null) {
                        Set<Document<E, K, T>> deleted = getChildrenToDelete(document.getDocuments(), currrent.getDocuments());
                        if (deleted != null && !deleted.isEmpty()) {
                            for (Document<E, K, T> child : deleted) {
                                if (!delete(child.getId(), type, context)) {
                                    DefaultLogger.error(String.format("Failed to delete nested document. [id=%s]",
                                            child.getId().stringKey()));
                                }
                            }
                        }
                    }
                }
                if (children != null) {
                    for (Document<E, K, T> child : children) {
                        if (child.getState().getState() == EEntityState.New) {
                            create(child, context);
                        } else {
                            update(child, context);
                        }
                    }
                }
            }
            return find(document.getId(),
                    (Class<? extends Document<E, K, T>>) document.getClass(),
                    false,
                    context);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }


    private <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> Set<Document<E, K, T>> getChildrenToDelete(Set<? extends Document<E, K, T>> updated,
                                                                                                                                 Set<? extends Document<E, K, T>> existing) {
        if (existing != null && !existing.isEmpty()) {
            Set<Document<E, K, T>> delete = new HashSet<>();
            Map<String, Boolean> exists = new HashMap<>();
            for (Document<E, K, T> doc : updated) {
                exists.put(doc.getId().stringKey(), true);
            }
            for (Document<E, K, T> doc : existing) {
                String key = doc.entityKey().stringKey();
                if (!exists.containsKey(key)) {
                    delete.add(doc);
                }
            }
            if (!delete.isEmpty()) {
                return delete;
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (state.isAvailable()) {
                state.setState(ProcessorState.EProcessorState.Stopped);
            }
            doClose();
            if (settings.isCleanOnExit()) {
                IOUtils.cleanDirectory(baseDir, true);
            }
        }
    }

    public <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> boolean delete(@NonNull DocumentId docId,
                                                                                                    @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                    @NonNull DocumentContext context) throws DataStoreException {
        try {
            checkState(ProcessorState.EProcessorState.Running);
            metrics.deleteCounter().increment();
            try (Timer t = new Timer(metrics.deleteTimer())) {
                return deleteDoc(docId, entityType, context);
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> Document<E, K, T> find(@NonNull DocumentId id,
                                                                                                            @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                            boolean download,
                                                                                                            DocumentContext context) throws DataStoreException {
        try {
            checkState(ProcessorState.EProcessorState.Running);
            metrics.readCounter().increment();
            try (Timer t = new Timer(metrics.readTimer())) {
                Document<E, K, T> document = findDoc(id, entityType, download, context);
                if (document != null && document.getDocumentCount() > 0) {
                    if (document.getDocuments() == null) {
                        try (Cursor<DocumentId, Document<E, K, T>> cursor
                                     = findChildDocs(document.getId(), entityType, context)) {
                            while (true) {
                                List<Document<E, K, T>> docs = cursor.nextPage();
                                if (docs == null || docs.isEmpty()) break;
                                document.addAll(docs);
                            }
                        }
                    }
                    document.validate();
                }
                return document;
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> Cursor<DocumentId, Document<E, K, T>> search(@NonNull AbstractDataStore.Q query,
                                                                                                                                  @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                                                  DocumentContext context) throws DataStoreException {
        try {
            checkState(ProcessorState.EProcessorState.Running);
            metrics.searchCounter().increment();
            try (Timer t = new Timer(metrics.searchTimer())) {
                return searchDocs(query, entityType, false, 0, settings().getBatchSize(), context);
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> Cursor<DocumentId, Document<E, K, T>> search(@NonNull AbstractDataStore.Q query,
                                                                                                                                  @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                                                  boolean download,
                                                                                                                                  int currentPage,
                                                                                                                                  int batchSize,
                                                                                                                                  DocumentContext context) throws DataStoreException {
        try {
            checkState(ProcessorState.EProcessorState.Running);
            metrics.searchCounter().increment();
            try (Timer t = new Timer(metrics.searchTimer())) {
                return searchDocs(query, entityType, download, currentPage, batchSize, context);
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public abstract void endSession() throws DataStoreException;
    
    protected abstract void doConfigure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException;

    protected abstract <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> Document<E, K, T> createDoc(@NonNull Document<E, K, T> document,
                                                                                                                             @NonNull DocumentContext context) throws DataStoreException;

    protected abstract <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> Document<E, K, T> updateDoc(@NonNull Document<E, K, T> document,
                                                                                                                             @NonNull DocumentContext context) throws DataStoreException;

    protected abstract <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> boolean deleteDoc(@NonNull DocumentId id,
                                                                                                                   @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                                   @NonNull DocumentContext context) throws DataStoreException;

    protected abstract <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> Map<DocumentId, Boolean> deleteDocs(@NonNull List<DocumentId> ids,
                                                                                                                                     @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                                                     @NonNull DocumentContext context) throws DataStoreException;

    protected abstract <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> Document<E, K, T> findDoc(@NonNull DocumentId docId,
                                                                                                                           @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                                           boolean download,
                                                                                                                           DocumentContext context) throws DataStoreException;

    protected abstract <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> Cursor<DocumentId, Document<E, K, T>> findChildDocs(@NonNull DocumentId docId,
                                                                                                                                                     @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                                                                     DocumentContext context) throws DataStoreException;

    protected abstract <E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>> Cursor<DocumentId, Document<E, K, T>> searchDocs(@NonNull AbstractDataStore.Q query,
                                                                                                                                                  @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                                                                  boolean download,
                                                                                                                                                  int currentPage,
                                                                                                                                                  int batchSize,
                                                                                                                                                  DocumentContext context) throws DataStoreException;

    protected abstract void doClose() throws IOException;
}
