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

package io.zyient.core.persistence.env;

import com.google.common.base.Preconditions;
import io.zyient.base.common.AbstractEnvState;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.env.BaseEnvSettings;
import io.zyient.core.filesystem.env.FileSystemEnv;
import io.zyient.core.persistence.DataStoreManager;
import io.zyient.core.persistence.DataStoreProvider;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class DataStoreEnv<T extends Enum<?>> extends FileSystemEnv<T> implements DataStoreProvider {
    private DataStoreManager dataStoreManager;

    public DataStoreEnv(@NonNull String name) {
        super(name);
    }

    @Override
    public BaseEnv<T> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull AbstractEnvState<T> state,
                           @NonNull Class<? extends BaseEnvSettings> type) throws ConfigurationException {
        Preconditions.checkArgument(ReflectionHelper.isSuperType(DataStoreEnvSettings.class, type));
        super.init(xmlConfig, state, type);
        try {
            DataStoreEnvSettings settings = (DataStoreEnvSettings) settings();
            HierarchicalConfiguration<ImmutableNode> config = config().config();
            if (ConfigReader.checkIfNodeExists(config, settings.getDataStoresPath())) {
                dataStoreManager = new DataStoreManager()
                        .init(config, this, settings.getDataStoresPath());
            } else {
                DefaultLogger.warn("No DataStores specified...");
            }
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public DataStoreManager getDataStoreManager() throws ConfigurationException {
        return dataStoreManager;
    }

    @Override
    public void close() throws Exception {
        if (dataStoreManager != null) {
            dataStoreManager.closeStores();
            dataStoreManager = null;
        }
        super.close();
    }
}
