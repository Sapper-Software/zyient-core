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

package io.zyient.base.core.connections.aws;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.DemoEnv;
import io.zyient.base.core.connections.ConnectionManager;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AwsS3ConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/aws/aws-connection-test.xml";
    private static final String __CONNECTION_NAME = "test-aws";
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
        try {
            AwsS3Connection connection = manager.getConnection(__CONNECTION_NAME, AwsS3Connection.class);
            assertNotNull(connection);
            connection.connect();
            assertTrue(connection.isConnected());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}