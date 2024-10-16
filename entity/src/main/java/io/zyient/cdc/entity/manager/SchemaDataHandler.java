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

package io.zyient.cdc.entity.manager;

import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.DistributedLock;
import io.zyient.base.core.connections.Connection;
import io.zyient.cdc.entity.schema.Domain;
import io.zyient.cdc.entity.schema.EntitySchema;
import io.zyient.cdc.entity.schema.SchemaEntity;
import io.zyient.cdc.entity.schema.SchemaVersion;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class SchemaDataHandler implements Closeable {
    private final Class<? extends SchemaDataHandlerSettings> settingsType;
    private BaseEnv<?> env;
    protected Connection connection;
    private SchemaDataHandlerSettings settings;

    protected SchemaDataHandler(@NonNull Class<? extends SchemaDataHandlerSettings> settingsType) {
        this.settingsType = settingsType;
    }

    public SchemaDataHandler init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                  @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(xmlConfig, settingsType);
            reader.read();
            return init((SchemaDataHandlerSettings) reader.settings(), env);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public SchemaDataHandler init(@NonNull SchemaDataHandlerSettings settings,
                                  @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            this.env = env;
            this.settings = settings;
            init(settings);
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public abstract void init(@NonNull SchemaDataHandlerSettings settings) throws Exception;


    protected abstract Domain saveDomain(@NonNull Domain domain) throws Exception;

    protected abstract Domain fetchDomain(@NonNull String domain) throws Exception;

    protected abstract boolean deleteDomain(@NonNull Domain domain) throws Exception;

    protected abstract SchemaEntity fetchEntity(@NonNull String domain, @NonNull String entity) throws Exception;

    protected abstract SchemaEntity saveEntity(@NonNull SchemaEntity entity) throws Exception;

    protected abstract boolean deleteEntity(@NonNull SchemaEntity entity) throws Exception;

    protected abstract EntitySchema fetchSchema(@NonNull SchemaEntity entity,
                                                SchemaVersion version) throws Exception;

    protected abstract EntitySchema fetchSchema(@NonNull SchemaEntity entity,
                                                @NonNull String uri) throws Exception;

    protected abstract EntitySchema saveSchema(@NonNull EntitySchema schema) throws Exception;

    protected abstract boolean deleteSchema(@NonNull SchemaEntity entity,
                                            @NonNull SchemaVersion version) throws Exception;


    public abstract String getSchemaEntityURI(@NonNull SchemaEntity entity,
                                              SchemaVersion version) throws Exception;

    protected abstract List<Domain> listDomains() throws Exception;

    protected abstract SchemaVersion parseVersion(@NonNull SchemaEntity entity,
                                                  @NonNull String uri) throws Exception;

    public abstract DistributedLock schemaLock(@NonNull SchemaEntity entity) throws Exception;
}
