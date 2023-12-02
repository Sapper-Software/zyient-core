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

package io.zyient.base.core.utils;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.ConnectionManager;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
public class ConnectionLoader {
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configFile;
    @Parameter(names = {"--path", "-p"}, description = "Connection definition path.")
    private String configPath = null;
    @Parameter(names = {"--type", "-t"}, description = "Configuration file type. (File, Resource, Remote)")
    private String configSource;

    private EConfigFileType fileSource = EConfigFileType.File;
    private HierarchicalConfiguration<ImmutableNode> config;

    public void run() throws Exception {
        if (!Strings.isNullOrEmpty(configSource)) {
            fileSource = EConfigFileType.parse(configSource);
        }
        Preconditions.checkNotNull(fileSource);
        config = ConfigReader.read(configFile, fileSource);

        UtilsEnv env = new UtilsEnv(UtilsEnv.__DEFAULT_NAME);
        env.init(config, new UtilsEnv.UtilsState(), UtilsEnv.UtilsEnvSettings.class);
        try (ConnectionManager manager = new ConnectionManager()) {
            manager.init(config, env, env.name());
            manager.save();
        }
    }

    public static void main(String[] args) {
        try {
            ConnectionLoader loader = new ConnectionLoader();
            JCommander.newBuilder().addObject(loader).build().parse(args);
            loader.run();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(t.getLocalizedMessage());
            t.printStackTrace();
        }
    }
}
