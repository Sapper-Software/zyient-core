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

package io.zyient.core.filesystem.env;

import io.zyient.base.common.AbstractEnvState;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.env.BaseEnvSettings;
import io.zyient.core.filesystem.FileSystemManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public abstract class FileSystemEnv<T extends Enum<?>> extends BaseEnv<T> {
    private FileSystemManager fileSystemManager;

    public FileSystemEnv(@NonNull String name) {
        super(name);
    }

    @Override
    public BaseEnv<T> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull AbstractEnvState<T> state,
                           @NonNull Class<? extends BaseEnvSettings> type) throws ConfigurationException {
        super.init(xmlConfig, state, type);
        try {
            if (ConfigReader.checkIfNodeExists(baseConfig(), FileSystemManager.__CONFIG_PATH)) {
                fileSystemManager = new FileSystemManager();
                fileSystemManager.init(baseConfig(), this);
            }
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }
}
