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
import io.zyient.base.core.utils.FileSystemUtils;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class HdfsConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __CONNECTION_NAME = "test-hdfs";
    private static final String __DEST_FOLDER = "renamed";

    private static final String __PATH = UUID.randomUUID().toString();

    private static XMLConfiguration xmlConfiguration = null;

    private static ConnectionManager manager = new ConnectionManager();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = TestUtils.readFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
        DemoEnv env = new DemoEnv();
        env.create(xmlConfiguration);
        manager.init(xmlConfiguration, env, env.name());
    }

    @Test
    void connect() {
        DefaultLogger.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            HdfsConnection connection = manager.getConnection(__CONNECTION_NAME, HdfsConnection.class);
            connection.connect();

            FileSystem fs = connection.fileSystem();
            assertNotNull(fs);
            Path path = new Path(String.format("/test/hcdc/core/%s", __PATH));
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
            List<Path> paths = FileSystemUtils.list("/", fs);
            assertNotNull(paths);
            for (Path p : paths) {
                URI uri = p.toUri();
                String hdfsPath = uri.getPath();
                DefaultLogger.info(String.format("HDFS Path=[%s]", hdfsPath));
            }
            manager.save(connection);
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }

    @Test
    void fileLifeCycle() {
        DefaultLogger.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "close"));
        try {
            HdfsConnection connection = manager.getConnection(__CONNECTION_NAME, HdfsConnection.class);
            connection.connect();

            FileSystem fs = connection.fileSystem();
            assertNotNull(fs);
            Path path = new Path(String.format("/test/hcdc/core/%s", __PATH));
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
            path = new Path(String.format("/test/hcdc/core/%s/upload.xml", __PATH));
            try (FSDataOutputStream fsDataOutputStream = fs.create(path, true)) {
                try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))) {
                    for (int ii = 0; ii < 50000; ii++) {
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
                    writer.flush();
                }
            }
            try (InputStream is = fs.open(path)) {
                byte[] buffer = new byte[4096];
                int l = is.read(buffer, 0, 4096);
            }

            try (FSDataOutputStream fsDataOutputStream = fs.append(path)) {
                try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))) {
                    for (int ii = 0; ii < 70000; ii++) {
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
                    writer.flush();
                }
            }

            String dd = String.format("%s/%s", path.getParent(), __DEST_FOLDER);
            Path dest = new Path(dd);
            fs.mkdirs(dest);
            Path df = new Path(String.format("%s/%s", dd, path.getName()));

            fs.rename(path, df);

            //fs.truncate(df, 50000);
            //fs.truncate(df, 0);

            //fs.delete(df, false);

            connection.close();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}