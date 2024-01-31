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

package io.zyient.base.core.auditing.loggers;

import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.auditing.*;
import io.zyient.base.core.auditing.writers.IAuditWriter;
import io.zyient.base.core.keystore.KeyStore;
import io.zyient.base.core.model.UserOrRole;
import io.zyient.base.core.processing.ProcessorState;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class SyncAuditLogger<T extends AuditRecord<R>, R> implements IAuditLogger<T> {
    private final ProcessorState state = new ProcessorState();
    private final Class<? extends AuditLoggerSettings> settingsType;

    private IAuditWriter<T> writer;
    private IAuditSerDe<R> serializer;
    private AuditReader<T, R> reader;

    private AuditLoggerSettings settings;
    private BaseEnv<?> env;
    private EncryptionInfo encryption;
    private KeyStore keyStore;

    public SyncAuditLogger() {
        settingsType = AuditLoggerSettings.class;
    }

    public SyncAuditLogger(Class<? extends AuditLoggerSettings> settingsType) {
        this.settingsType = settingsType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public IAuditLogger<T> init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                @NonNull BaseEnv<?> env) throws ConfigurationException {
        this.env = env;
        try {
            ConfigReader reader = new ConfigReader(config, settingsType);
            reader.read();
            settings = (AuditLoggerSettings) reader.settings();
            writer = (IAuditWriter<T>) settings.getWriter().getDeclaredConstructor()
                    .newInstance();
            HierarchicalConfiguration<ImmutableNode> node = reader.config().configurationAt(IAuditWriter.__CONFIG_PATH);
            writer.name(settings.getName())
                    .init(node, env, (Class<? extends T>) settings.getRecordType());
            serializer = (IAuditSerDe<R>) settings.getSerializer().getDeclaredConstructor()
                    .newInstance();
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
            if (ConfigReader.checkIfNodeExists(reader.config(), AuditReader.__CONFIG_PATH)) {
                node = reader.config().configurationAt(AuditReader.__CONFIG_PATH);
                Class<? extends AuditReader<T, R>> rt
                        = (Class<? extends AuditReader<T, R>>) ConfigReader.readType(reader.config());
                this.reader = rt.getDeclaredConstructor()
                        .newInstance();
                this.reader.env(env)
                        .recordType((Class<? extends T>) settings.getRecordType())
                        .name(settings.getName())
                        .serializer(serializer)
                        .encryption(encryption)
                        .init(node);
            }
            state.setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void audit(@NonNull Object record,
                      @NonNull AuditRecordType type,
                      @NonNull String module,
                      @NonNull UserOrRole user,
                      Context context) throws Exception {
        state.check(ProcessorState.EProcessorState.Running);

        R data = serializer.serialize(record,
                encryption,
                keyStore);

        AuditRecord<R> rec = (AuditRecord<R>) settings.getRecordType()
                .getDeclaredConstructor()
                .newInstance();
        rec.setNamespace(env.name());
        rec.setModule(module);
        rec.setInstanceId(env.moduleInstance().id());
        rec.setActor(new ActionedBy(user));
        rec.setRecordType(record.getClass().getCanonicalName());
        rec.data(data);
        writer.write((T) rec, context);
    }

    @Override
    public void close() throws IOException {
        if (!state.hasError()) {
            state.setState(ProcessorState.EProcessorState.Stopped);
        }
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }
}
