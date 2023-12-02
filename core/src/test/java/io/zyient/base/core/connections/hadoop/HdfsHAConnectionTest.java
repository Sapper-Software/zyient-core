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

package io.zyient.base.core.connections.hadoop;

import com.google.common.base.Preconditions;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.DemoEnv;
import io.zyient.base.core.connections.ConnectionManager;
import io.zyient.base.core.connections.TestUtils;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class HdfsHAConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __CONNECTION_NAME = "test-hdfs-ha";

    private static final String __UUID = UUID.randomUUID().toString();
    private static final String __PATH = String.format("/test/hcdc/core/HA/%s", __UUID);
    private static XMLConfiguration xmlConfiguration = null;
    private static ConnectionManager manager = new ConnectionManager();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = TestUtils.readFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
        DemoEnv env = new DemoEnv();
        env.init(xmlConfiguration);
        manager.init(xmlConfiguration, env, env.name());
    }

    @Test
    void connect() {
        DefaultLogger.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            HdfsHAConnection connection = manager.getConnection(__CONNECTION_NAME, HdfsHAConnection.class);
            connection.connect();

            FileSystem fs = connection.fileSystem();
            assertNotNull(fs);
            Path path = new Path(__PATH);
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
            manager.save(connection);
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }

    @Test
    void close() {
        DefaultLogger.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "close"));
        try {
            HdfsHAConnection connection = manager.getConnection(__CONNECTION_NAME, HdfsHAConnection.class);
            connection.connect();

            FileSystem fs = connection.fileSystem();
            assertNotNull(fs);
            Path path = new Path(__PATH);
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
            path = new Path(String.format("%s/upload.xml", __PATH));
            FSDataOutputStream fsDataOutputStream = fs.create(path, true);
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))) {
                File file = new File(__CONFIG_FILE);    //creates a new file instance
                FileReader fr = new FileReader(file);   //reads the file
                try (BufferedReader reader = new BufferedReader(fr)) {  //creates a buffering character input stream
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.write(line);
                        writer.newLine();
                    }
                }
            }

            connection.close();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}