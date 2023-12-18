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

package io.zyient.base.core.connections.db;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.DemoEnv;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

class OracleConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/test-oracle-env.xml";
    private static XMLConfiguration xmlConfiguration = null;
    private static DemoEnv env = new DemoEnv();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.create(xmlConfiguration);
    }

    @AfterAll
    public static void stop() throws Exception {
        env.close();
    }

    @Test
    void connect() {
        try {
            OracleConnection connection = env.connectionManager().getConnection("test-oracle", OracleConnection.class);
            assertNotNull(connection);
            connection.connect();
            assertTrue(connection.isConnected());
            try (Connection sqlc = connection.getConnection()) {
                Statement stmt = sqlc.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT sysdate FROM dual");
                rs.next();
                Timestamp ts = rs.getTimestamp(1);
                System.out.println(ts);
            }
            env.connectionManager().save();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}