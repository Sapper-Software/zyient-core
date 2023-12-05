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

package io.zyient.base.core.content;

import com.google.common.base.Preconditions;
import io.zyient.base.common.StateException;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.IOUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.model.UserContext;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.Cursor;
import io.zyient.base.core.stores.DataStoreException;
import io.zyient.base.core.stores.model.Document;
import io.zyient.base.core.stores.model.DocumentId;
import io.zyient.base.core.utils.Timer;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class ContentProvider implements Closeable {
    public static final String KEY_ENGINE = "ContentProvider";
    public static final String __CONFIG_PATH = "contentManager";

    private final ProcessorState state = new ProcessorState();
    private final Class<? extends ContentProviderSettings> settingsType;

    private ContentProviderSettings settings;
    private File baseDir;
    private BaseEnv<?> env;
    private ContentProviderMetrics metrics;

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

    public <E extends Enum<?>, K extends IKey, T extends Document<E, K, T>> Document<E, K, T> create(@NonNull Document<E, K, T> document,
                                                                                                     @NonNull Context context) throws DataStoreException {
        try {
            checkState(ProcessorState.EProcessorState.Running);
            if (!(context instanceof UserContext)) {
                throw new DataStoreException(String.format("User context expected. [doc id=%s]",
                        document.getId().stringKey()));
            }
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
                UserContext uc = (UserContext) context;
                document.setCreatedBy(uc.user().getName());
                document.setModifiedBy(uc.user().getName());
                document.setCreatedTime(System.nanoTime());
                document.setUpdatedTime(System.nanoTime());
                document.validate();

                document = createDoc(document, context);
            }
            return document;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    public <E extends Enum<?>, K extends IKey, T extends Document<E, K, T>> Document<E, K, T> update(@NonNull Document<E, K, T> document,
                                                                                                     @NonNull Context context) throws DataStoreException {
        try {
            checkState(ProcessorState.EProcessorState.Running);
            if (!(context instanceof UserContext)) {
                throw new DataStoreException(String.format("User context expected. [doc id=%s]",
                        document.getId().stringKey()));
            }
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
                UserContext uc = (UserContext) context;
                document.setModifiedBy(uc.user().getName());
                document.setUpdatedTime(System.nanoTime());
                document = updateDoc(document, context);
                document.validate();
            }
            return document;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
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

    public <E extends Enum<?>, K extends IKey, T extends Document<E, K, T>> boolean delete(@NonNull DocumentId docId,
                                                                                           @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                           @NonNull Context context) throws DataStoreException {
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

    public <E extends Enum<?>, K extends IKey, T extends Document<E, K, T>> Document<E, K, T> find(@NonNull DocumentId id,
                                                                                                   @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                   Context context) throws DataStoreException {
        try {
            checkState(ProcessorState.EProcessorState.Running);
            metrics.readCounter().increment();
            try (Timer t = new Timer(metrics.readTimer())) {
                return findDoc(id, entityType, context);
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public <E extends Enum<?>, K extends IKey, T extends Document<E, K, T>> Cursor<DocumentId, Document<E, K, T>> search(@NonNull AbstractDataStore.Q query,
                                                                                                                         @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                                         Context context) throws DataStoreException {
        try {
            checkState(ProcessorState.EProcessorState.Running);
            metrics.searchCounter().increment();
            try (Timer t = new Timer(metrics.searchTimer())) {
                return searchDocs(query, entityType, settings().getBatchSize(), false, context);
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public <E extends Enum<?>, K extends IKey, T extends Document<E, K, T>> Cursor<DocumentId, Document<E, K, T>> search(@NonNull AbstractDataStore.Q query,
                                                                                                                         @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                                         int batchSize,
                                                                                                                         boolean download,
                                                                                                                         Context context) throws DataStoreException {
        try {
            checkState(ProcessorState.EProcessorState.Running);
            metrics.searchCounter().increment();
            try (Timer t = new Timer(metrics.searchTimer())) {
                return searchDocs(query, entityType, batchSize, download, context);
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    protected abstract void doConfigure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException;

    protected abstract <E extends Enum<?>, K extends IKey, T extends Document<E, K, T>> Document<E, K, T> createDoc(@NonNull Document<E, K, T> document,
                                                                                                                    @NonNull Context context) throws DataStoreException;

    protected abstract <E extends Enum<?>, K extends IKey, T extends Document<E, K, T>> Document<E, K, T> updateDoc(@NonNull Document<E, K, T> document,
                                                                                                                    @NonNull Context context) throws DataStoreException;

    protected abstract <E extends Enum<?>, K extends IKey, T extends Document<E, K, T>> boolean deleteDoc(@NonNull DocumentId id,
                                                                                                          @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                          @NonNull Context context) throws DataStoreException;

    protected abstract <E extends Enum<?>, K extends IKey, T extends Document<E, K, T>> Map<DocumentId, Boolean> deleteDocs(@NonNull List<DocumentId> ids,
                                                                                                                            @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                                            @NonNull Context context) throws DataStoreException;

    protected abstract <E extends Enum<?>, K extends IKey, T extends Document<E, K, T>> Document<E, K, T> findDoc(@NonNull DocumentId docId,
                                                                                                                  @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                                  Context context) throws DataStoreException;

    protected abstract <E extends Enum<?>, K extends IKey, T extends Document<E, K, T>> Cursor<DocumentId, Document<E, K, T>> searchDocs(@NonNull AbstractDataStore.Q query,
                                                                                                                                         @NonNull Class<? extends Document<E, K, T>> entityType,
                                                                                                                                         int batchSize,
                                                                                                                                         boolean download,
                                                                                                                                         Context context) throws DataStoreException;

    protected abstract void doClose() throws IOException;
}
