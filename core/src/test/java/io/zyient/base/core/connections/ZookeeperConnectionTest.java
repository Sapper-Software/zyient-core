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

package io.zyient.base.core.connections;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.DemoEnv;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.common.ZookeeperSettings;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.curator.utils.ZKPaths;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ZookeeperConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __CONNECTION_NAME = "test-zk";
    private static final String __UUID = UUID.randomUUID().toString();
    private static final String __BASE_PATH = "/test/hcdc/core/zookeeper";
    private static final String __PATH = String.format("%s/%s", __BASE_PATH, __UUID);

    private static XMLConfiguration xmlConfiguration = null;

    private static ConnectionManager manager;
    private static DemoEnv env = new DemoEnv();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.create(xmlConfiguration);
        manager = env.connectionManager();
    }

    @AfterAll
    public static void stop() throws Exception {
        env.close();
    }


    @Test
    void connect() {
        DefaultLogger.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            ZookeeperConnection connection = manager.getConnection(__CONNECTION_NAME, ZookeeperConnection.class);
            assertNotNull(connection);
            connection.connect();

            manager.save(connection);
            assertEquals(Connection.EConnectionState.Connected, connection.connectionState());
            ZKPaths.mkdirs(connection.client().getZookeeperClient().getZooKeeper(), __PATH);

            List<String> paths = connection.client().getChildren().forPath(__BASE_PATH);
            assertNotNull(paths);
            DefaultLogger.debug(String.format("PATHS : [%s]", paths));

            connection.close();
            assertEquals(Connection.EConnectionState.Closed, connection.connectionState());

            ZookeeperSettings settings = connection.settings();
            Map<String, String> values = settings.serialize();
            assertNotNull(values);
            assertFalse(values.isEmpty());
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}