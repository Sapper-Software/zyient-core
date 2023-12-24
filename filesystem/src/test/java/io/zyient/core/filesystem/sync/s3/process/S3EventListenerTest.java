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

package io.zyient.core.filesystem.sync.s3.process;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.aws.AwsS3Connection;
import io.zyient.base.core.utils.FileUtils;
import io.zyient.core.filesystem.env.DemoFileSystemEnv;
import io.zyient.core.filesystem.impl.s3.S3Helper;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class S3EventListenerTest {
    private static final String __CONFIG_FILE = "src/test/resources/s3/sync-s3-test.xml";
    private static final String S3_CONNECTION = "test-s3";
    private static final String S3_BUCKET = "zyient-events-test";
    private static final String BASE_PATH = "test/data";
    private static final String FILES_DIR = "src/test/resources/data";
    private static XMLConfiguration xmlConfiguration = null;
    private static DemoFileSystemEnv env = new DemoFileSystemEnv();
    private static AwsS3Connection connection;
    private static S3EventListener listener;
    private static List<File> files;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.create(xmlConfiguration);
        connection = env.connectionManager()
                .getConnection(S3_CONNECTION, AwsS3Connection.class);
        assertNotNull(connection);
        if (!connection.isConnected())
            connection.connect();
        listener = new S3EventListener();
        HierarchicalConfiguration<ImmutableNode> node
                = env.configNode().configurationAt(S3EventListenerSettings.__CONFIG_PATH);
        listener.init(node, env);
        files = FileUtils.getFiles(new File(FILES_DIR));
    }

    @AfterAll
    public static void stop() throws Exception {
        if (listener != null) {
            listener.close();
        }
        env.close();
    }

    @Test
    void init() {
        try {
            String prefix = String.format("%s/%s/", BASE_PATH, UUID.randomUUID().toString());
            S3EventTestHandler handler = (S3EventTestHandler) listener.handler();
            assertNotNull(handler);
            Map<String, Boolean> uploaded = new HashMap<>();
            for (File file : files) {
                if (!file.isFile()) continue;
                String path = String.format("%s/%s", prefix, file.getName());
                S3Helper.upload(connection.client(),
                        S3_BUCKET,
                        path,
                        file);
                uploaded.put(path, true);
            }
            Thread.sleep(1000);
            while (true) {
                Map<String, Boolean> received = handler.received();
                if (!received.isEmpty()) {
                    for (String path : received.keySet()) {
                        uploaded.remove(path);
                    }
                }
                if (uploaded.isEmpty()) break;
                Thread.sleep(500);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}