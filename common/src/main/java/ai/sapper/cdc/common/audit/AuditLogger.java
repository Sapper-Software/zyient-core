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

import com.google.protobuf.MessageOrBuilder;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.util.List;

public interface AuditLogger {
    public static final String __CONFIG_PATH = "audit";
    public static final String CONFIG_AUDIT_CLASS = String.format("%s.class", __CONFIG_PATH);

    AuditLogger init(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException;

    <T> void audit(@NonNull Class<?> caller, long timestamp, @NonNull T data);

    void audit(@NonNull Class<?> caller, long timestamp, @NonNull MessageOrBuilder data);

    List<AuditRecord> read(@NonNull String logfile, int offset, int batchSize) throws IOException;
}
