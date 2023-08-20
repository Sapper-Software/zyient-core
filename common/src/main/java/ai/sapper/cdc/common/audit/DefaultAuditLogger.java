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

package ai.sapper.cdc.common.audit;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DefaultAuditLogger implements AuditLogger {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuditLogger.class);

    @Override
    public AuditLogger init(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        return this;
    }

    @Override
    public <T> void audit(@NonNull Class<?> caller, long timestamp, @NonNull T data) {
        try {
            AuditRecord record = new AuditRecord();
            record.setCaller(caller.getCanonicalName());
            record.setType(data.getClass().getCanonicalName());
            record.setTimestamp(timestamp);
            record.setData(data);

            String json = JSONUtils.asString(record, record.getClass());
            LOGGER.info(String.format("RECORD[%s]", json));
        } catch (Exception ex) {
            DefaultLogger.error(ex.getLocalizedMessage());
            DefaultLogger.stacktrace(ex);
        }
    }

    @Override
    public void audit(@NonNull Class<?> caller, long timestamp, @NonNull MessageOrBuilder data) {
        try {
            String ds = JsonFormat.printer().print(data);
            AuditRecord<String> record = new AuditRecord<>();
            record.setCaller(caller.getCanonicalName());
            record.setType(data.getClass().getCanonicalName());
            record.setTimestamp(timestamp);
            record.setData(ds);

            String json = JSONUtils.asString(record, record.getClass());
            LOGGER.info(String.format("RECORD[%s]", json));
        } catch (Exception ex) {
            DefaultLogger.error(ex.getLocalizedMessage());
            DefaultLogger.stacktrace(ex);
        }
    }

    @Override
    public List<AuditRecord<?>> read(@NonNull String logfile, int offset, int batchSize) throws IOException {
        return null;
    }
}
