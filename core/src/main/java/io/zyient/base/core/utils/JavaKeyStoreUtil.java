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
import io.zyient.base.core.keystore.KeyStore;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
public class JavaKeyStoreUtil {
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configFile;
    @Parameter(names = {"--type", "-t"}, description = "Configuration file type. (File, Resource, Remote)")
    private String configSource;
    @Parameter(names = {"--key", "-k"}, required = true, description = "Key Name (Alias)")
    private String key;
    @Parameter(names = {"--value", "-v"}, required = true, description = "Value to store.")
    private String value;
    @Parameter(names = {"--passwd", "-p"}, required = true, description = "Key Store password.")
    private String password;
    private EConfigFileType fileSource = EConfigFileType.File;
    @Setter(AccessLevel.NONE)
    private DemoEnv env = new DemoEnv();

    @SuppressWarnings("unchecked")
    public void run() throws Exception {
        if (!Strings.isNullOrEmpty(configSource)) {
            fileSource = EConfigFileType.parse(configSource);
        }
        Preconditions.checkNotNull(fileSource);
        HierarchicalConfiguration<ImmutableNode> config = ConfigReader.read(configFile, fileSource);
        env.withStoreKey(password);
        env.init(config);

        config = env.baseConfig();

        String c = config.getString(KeyStore.CONFIG_KEYSTORE_CLASS);
        if (Strings.isNullOrEmpty(c)) {
            throw new ConfigurationException(
                    String.format("Key Store class not defined. [config=%s]", KeyStore.CONFIG_KEYSTORE_CLASS));
        }
        Class<? extends KeyStore> cls = (Class<? extends KeyStore>) Class.forName(c);
        KeyStore keyStore = cls.getDeclaredConstructor().newInstance();
        keyStore.withPassword(password)
                .init(config, env);
        keyStore.save(key, value);
        keyStore.flush();
    }

    public static void main(String[] args) {
        try {
            JavaKeyStoreUtil util = new JavaKeyStoreUtil();
            JCommander.newBuilder().addObject(util).build().parse(args);
            util.run();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(t.getLocalizedMessage());
            t.printStackTrace();
        }
    }
}
