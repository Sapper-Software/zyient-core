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

import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.auditing.AuditRecord;
import io.zyient.base.core.auditing.AuditRecordType;
import io.zyient.base.core.auditing.IAuditLogger;
import io.zyient.base.core.model.UserOrRole;
import io.zyient.base.core.processing.ProcessorState;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AsyncAuditLogger<T extends AuditRecord<R>, R> extends SyncAuditLogger<T, R> {
    public static class AuditData {
        private Object record;
        private AuditRecordType type;
        private String module;
        private UserOrRole user;
        private Context context;
    }


    private ExecutorService executor;

    public AsyncAuditLogger() {
        super(AsyncAuditLoggerSettings.class);
    }

    @Override
    public IAuditLogger<T> init(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull BaseEnv<?> env) throws ConfigurationException {
        super.init(config, env);
        try {
            AsyncAuditLoggerSettings settings = (AsyncAuditLoggerSettings) settings();
            executor = new ThreadPoolExecutor(settings.getPoolSize(),
                    settings.getPoolSize(), 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(settings.getPoolQueueSize()));

            return this;
        } catch (Exception ex) {
            state().error(ex);
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void audit(@NonNull Object record,
                      @NonNull AuditRecordType type,
                      @NonNull String module,
                      @NonNull UserOrRole user,
                      Context context) throws Exception {
        state().check(ProcessorState.EProcessorState.Running);
        AuditData data = new AuditData();
        data.record = record;
        data.type = type;
        data.module = module;
        data.user = user;
        data.context = context;
        executor.submit(new Task(data, this));
    }

    private void call(@NonNull AuditData data) throws Exception {
        super.audit(data.record, data.type, data.module, data.user, data.context);
    }

    @Override
    public void close() throws IOException {
        super.close();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2 * 1000, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    public static class Task implements Runnable {
        private final AuditData data;
        private final AsyncAuditLogger<?, ?> parent;

        public Task(AuditData data, AsyncAuditLogger<?, ?> parent) {
            this.data = data;
            this.parent = parent;
        }

        @Override
        public void run() {
            try {
                parent.call(data);
            } catch (Exception ex) {
                DefaultLogger.stacktrace(ex);
                DefaultLogger.error(ex.getLocalizedMessage());
            }
        }
    }
}
