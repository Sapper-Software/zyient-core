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

package io.zyient.core.persistence.auditing.writers;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.auditing.JsonAuditRecord;
import io.zyient.base.core.auditing.writers.IAuditWriter;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.persistence.impl.rdbms.HibernateConnection;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.hibernate.Session;
import org.hibernate.Transaction;

import java.io.IOException;

public class DbWriter implements IAuditWriter<JsonAuditRecord> {
    private final ProcessorState state = new ProcessorState();
    private HibernateConnection connection;
    private Class<? extends JsonAuditRecord> recordType;
    private String name;
    private DbWriterSettings settings;
    private long committedTimestamp = -1;
    private Session currentSession;
    private Transaction currentTx;
    private int pendingCommitCount = 0;

    @Override
    public IAuditWriter<JsonAuditRecord> init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                              @NonNull BaseEnv<?> env,
                                              @NonNull Class<? extends JsonAuditRecord> recordType) throws ConfigurationException {
        Preconditions.checkState(!Strings.isNullOrEmpty(name));
        try {
            ConfigReader reader = new ConfigReader(config, DbWriterSettings.class);
            reader.read();
            settings = (DbWriterSettings) reader.settings();
            connection = env.connectionManager()
                            .getConnection(settings.getConnection(), HibernateConnection.class);
            if (connection == null) {
                throw new Exception(String.format("DB Connection not found. [name=%s][type=%s]",
                        settings.getConnection(), HibernateConnection.class.getCanonicalName()));
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
    public void write(@NonNull JsonAuditRecord record, Context context) throws Exception {
        state.check(ProcessorState.EProcessorState.Running);
        record.validate();
        checkCommit();
        if (currentSession == null) {
            currentSession = connection.getConnection(false);
            currentTx = currentSession.beginTransaction();
        }
        currentSession.persist(record);
    }

    @Override
    public IAuditWriter<JsonAuditRecord> name(@NonNull String name) {
        this.name = name;
        return this;
    }

    @Override
    public String name() {
        return name;
    }

    private void checkCommit() throws Exception {
        if (committedTimestamp <= 0) {
            committedTimestamp = System.currentTimeMillis();
        } else if (currentTx != null) {
            long delta = System.currentTimeMillis() - committedTimestamp;
            if (pendingCommitCount >= settings.getCommitBatchSize()
                    || delta >= settings.getCommitTimeout().normalized()) {
                try {
                    currentTx.commit();
                } catch (Throwable t) {
                    state.error(t);
                    DefaultLogger.stacktrace(t);
                    throw new Exception(t);
                }
                currentSession.close();
                currentTx = null;
                currentSession = null;
                pendingCommitCount = 0;
                committedTimestamp = System.currentTimeMillis();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!state.hasError()) {
            state.setState(ProcessorState.EProcessorState.Stopped);
        }
        if (currentTx != null) {
            currentTx.commit();
            currentTx = null;
        }
        if (currentSession != null) {
            currentSession.close();
            currentSession = null;
        }
    }

}
