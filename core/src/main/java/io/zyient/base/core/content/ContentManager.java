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
import io.zyient.base.core.content.model.Document;
import io.zyient.base.core.content.model.DocumentId;
import io.zyient.base.core.model.UserContext;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.DataStoreException;
import io.zyient.base.core.stores.DataStoreManager;
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
public abstract class ContentManager implements Closeable {
    public static final String __CONFIG_PATH = "contentManager";

    private final ProcessorState state = new ProcessorState();
    private final Class<? extends ContentManagerSettings> settingsType;
    private final boolean supportsMetadata;

    private AbstractDataStore<?> metadataStore;
    private ContentManagerSettings settings;
    private File baseDir;

    protected ContentManager(@NonNull Class<? extends ContentManagerSettings> settingsType,
                             boolean supportsMetadata) {
        this.settingsType = settingsType;
        this.supportsMetadata = supportsMetadata;
    }

    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    public ContentManager configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                    DataStoreManager dataStoreManager) throws ConfigurationException {
        synchronized (this) {
            try {
                ConfigReader reader = new ConfigReader(xmlConfig, __CONFIG_PATH, settingsType);
                reader.read();
                settings = (ContentManagerSettings) reader.settings();
                settings.validate();

                if (settings().isSaveMetadata()) {
                    if (!supportsMetadata) {
                        DefaultLogger.warn(String.format("[%s] Metadata persistence not supported. [type=%s]",
                                settings.getName(), getClass().getCanonicalName()));
                    } else {
                        if (dataStoreManager == null) {
                            throw new Exception(String.format("[%s] Data Store manager not specified...",
                                    settings.getName()));
                        }
                        metadataStore = dataStoreManager.getDataStore(settings.getMetadataStore(),
                                settings.getMetadataStoreType());
                        if (metadataStore == null) {
                            throw new Exception(String.format("[%s] Data Store not found. [name=%s][type=%s]",
                                    settings.getName(),
                                    settings.getMetadataStore(),
                                    settings.getMetadataStoreType().getCanonicalName()));
                        }
                    }
                }
                baseDir = new File(settings.getBaseDir());
                if (!baseDir.exists()) {
                    if (!baseDir.mkdirs()) {
                        throw new IOException(String.format("Failed to create local directory. [path=%s]",
                                baseDir.getAbsolutePath()));
                    }
                }
                configure(reader.config());
                state.setState(ProcessorState.EProcessorState.Running);
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

    public <E extends Enum<?>, K extends IKey> Document<E, K> create(@NonNull Document<E, K> document,
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
            UserContext uc = (UserContext) context;
            document.setCreatedBy(uc.user().getName());
            document.setModifiedBy(uc.user().getName());
            document.setCreatedTime(System.nanoTime());
            document.setUpdatedTime(System.nanoTime());
            document.validate();

            document = createDoc(document, context);
            return document;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new DataStoreException(ex);
        }
    }

    public <E extends Enum<?>, K extends IKey> Document<E, K> update(@NonNull Document<E, K> document,
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
            UserContext uc = (UserContext) context;
            document.setModifiedBy(uc.user().getName());
            document.setUpdatedTime(System.nanoTime());
            document = updateDoc(document, context);
            document.validate();

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
            if (settings.isCleanOnExit()) {
                IOUtils.cleanDirectory(baseDir, true);
            }
        }
    }

    public boolean delete(@NonNull DocumentId docId, @NonNull Context context) throws DataStoreException {
        return deleteDoc(docId, context);
    }

    protected abstract void configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException;

    protected abstract <E extends Enum<?>, K extends IKey> Document<E, K> createDoc(@NonNull Document<E, K> document,
                                                                                    @NonNull Context context) throws DataStoreException;

    protected abstract <E extends Enum<?>, K extends IKey> Document<E, K> updateDoc(@NonNull Document<E, K> document,
                                                                                    @NonNull Context context) throws DataStoreException;

    protected abstract boolean deleteDoc(@NonNull DocumentId id,
                                         @NonNull Context context) throws DataStoreException;

    protected abstract Map<DocumentId, Boolean> deleteDocs(@NonNull List<DocumentId> ids,
                                                           @NonNull Context context) throws DataStoreException;

    protected abstract <E extends Enum<?>, K extends IKey> Document<E, K> findDoc(@NonNull DocumentId docId,
                                                                                  Context context) throws DataStoreException;

    protected abstract <E extends Enum<?>, K extends IKey> DocumentCursor<E, K> searchDocs(@NonNull AbstractDataStore.Q query,
                                                                                           Context context) throws DataStoreException;
}
