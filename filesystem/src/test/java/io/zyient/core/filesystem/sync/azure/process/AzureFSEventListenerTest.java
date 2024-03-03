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

package io.zyient.core.filesystem.sync.azure.process;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.core.connections.azure.AzureFsClient;
import io.zyient.base.core.processing.ProcessorSettings;
import io.zyient.core.filesystem.env.DemoFileSystemEnv;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class AzureFSEventListenerTest {
    private static final String __CONFIG_FILE = "src/test/resources/azure/fs-events-test.xml";
    private static final String FILES_DIR = "src/test/resources/data";
    private static final String AZURE_FS_CLIENT = "test-azure-fs";
    private static XMLConfiguration xmlConfiguration = null;
    private static DemoFileSystemEnv env = new DemoFileSystemEnv();
    private static AzureFsClient client;
    private static AzureFSEventListener listener;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.create(xmlConfiguration);
        client = env.connectionManager()
                .getConnection(AZURE_FS_CLIENT, AzureFsClient.class);
        Preconditions.checkNotNull(client);
        HierarchicalConfiguration<ImmutableNode> node
                = env.configNode().configurationAt(ProcessorSettings.__CONFIG_PATH);
        listener = (AzureFSEventListener) new AzureFSEventListener()
                .init(env, env.configNode());
        listener.start();
    }


    @AfterAll
    public static void stop() throws Exception {
        env.close();
    }

    @Test
    void doRun() {
    }
}