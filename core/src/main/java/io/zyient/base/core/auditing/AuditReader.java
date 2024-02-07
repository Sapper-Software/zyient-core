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

package io.zyient.base.core.auditing;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.auditing.readers.AuditReaderSettings;
import io.zyient.base.core.keystore.KeyStore;
import io.zyient.base.core.processing.ProcessorState;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class AuditReader<T extends AuditRecord<R>, R> implements Closeable {
    public static final String __CONFIG_PATH = "reader";

    private final ProcessorState state = new ProcessorState();
    private final Class<? extends AuditReaderSettings> settingsType;

    private Class<? extends T> recordType;
    private IAuditSerDe<R> serializer;
    private EncryptionInfo encryption;
    private BaseEnv<?> env;
    @Setter(AccessLevel.NONE)
    private HierarchicalConfiguration<ImmutableNode> config;
    @Setter(AccessLevel.NONE)
    private AuditReaderSettings settings;
    private KeyStore keyStore;
    private String name;

    protected AuditReader(Class<? extends AuditReaderSettings> settingsType) {
        this.settingsType = settingsType;
    }

    @SuppressWarnings("unchecked")
    public AuditReader<T, R> init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                  @NonNull BaseEnv<?> env) throws ConfigurationException {
        this.env = env;
        try {
            ConfigReader reader = new ConfigReader(config, __CONFIG_PATH, settingsType);
            reader.read();
            settings = (AuditReaderSettings) reader.settings();
            this.config = reader.config();
            name = settings.getName();
            ConfigReader.checkStringValue(name, getClass(), "name");
            serializer = (IAuditSerDe<R>) settings.getSerializer().getDeclaredConstructor()
                    .newInstance();
            ConfigReader.checkNotNull(settings.getSerializer(), getClass(), "serializer");
            encryption = new EncryptionInfo();
            if (settings.isEncrypted()) {
                encryption.encrypted(true);
                keyStore = env.keyStore();
                if (keyStore == null) {
                    throw new Exception(String.format("No Key Store defined. [env=%s]", env.name()));
                }
                if (Strings.isNullOrEmpty(settings.getEncryptionKey())) {
                    throw new Exception("Encryption Key not defined...");
                }
                encryption.password(settings.getEncryptionKey());
                encryption.iv(keyStore.iv());
            }
            recordType = (Class<? extends T>) settings.getRecordType();
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    public AuditReader<T, R> init(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        Preconditions.checkState(!Strings.isNullOrEmpty(name));
        Preconditions.checkNotNull(env);
        Preconditions.checkNotNull(recordType);
        Preconditions.checkNotNull(serializer);
        Preconditions.checkNotNull(encryption);

        try {
            ConfigReader reader = new ConfigReader(config, AuditReaderSettings.class);
            reader.read();
            settings = (AuditReaderSettings) reader.settings();
            this.config = reader.config();

            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    public abstract List<T> read(int page,
                                 int batchSize,
                                 @NonNull EncryptionInfo encryption,
                                 Class<?> type,
                                 Context context) throws Exception;

    public abstract AuditCursor<T, R> read(long timeStart,
                                           long timeEnd,
                                           @NonNull EncryptionInfo encryption,
                                           Class<?> type,
                                           Context context) throws Exception;
}
