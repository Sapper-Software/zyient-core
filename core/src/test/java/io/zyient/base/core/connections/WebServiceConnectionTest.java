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

package io.zyient.base.core.connections;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.DemoEnv;
import io.zyient.base.core.connections.ws.WebServiceConnection;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.configuration2.XMLConfiguration;
import org.glassfish.jersey.client.JerseyWebTarget;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

class WebServiceConnectionTest {
    private static final String __CONFIG_FILE = "src/test/resources/connection-test.xml";
    private static final String __CONNECTION_NAME = "test-ws";

    private static XMLConfiguration xmlConfiguration = null;

    private static ConnectionManager manager;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = TestUtils.readFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
        DemoEnv env = new DemoEnv();
        env.init(xmlConfiguration);
        manager = env.connectionManager();
    }

    @Test
    void connect() {
        DefaultLogger.debug(String.format("Running [%s].%s()", getClass().getCanonicalName(), "connect"));
        try {
            WebServiceConnection ws = manager.getConnection(__CONNECTION_NAME, WebServiceConnection.class);

            JerseyWebTarget wt = ws.connect("jersey");
            Invocation.Builder builder = wt.request(MediaType.APPLICATION_JSON);
            Response response = builder.get();
            String data = response.readEntity(String.class);
            assertFalse(Strings.isNullOrEmpty(data));
            DefaultLogger.debug(String.format("DATA [%s]", data));

            manager.save(ws);
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}