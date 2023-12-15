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

package io.zyient.base.core.connections.ws;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.services.BooleanServiceResponse;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.TestUtils;
import io.zyient.base.core.services.test.WSTestEnv;
import io.zyient.base.core.services.test.model.DemoEntity;
import io.zyient.base.core.services.test.model.DemoEntityServiceResponse;
import io.zyient.base.core.utils.FileUtils;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class WebServiceClientTest {
    public static final String __CONFIG_FILE = "src/test/resources/ws/ws-client-test.xml";
    public static final String SERVICE_CREATE = "create";
    public static final String SERVICE_UPDATE = "update";
    public static final String SERVICE_DELETE = "delete";
    public static final String SERVICE_GET = "find";

    private static XMLConfiguration xmlConfiguration = null;
    private static WSTestEnv env;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = TestUtils.readFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
        env = new WSTestEnv();
        env.create(xmlConfiguration);
        Preconditions.checkNotNull(env.serviceClient());
    }

    public static void dispose() throws Exception {
        env.close();
    }

    @Test
    void get() {
        try {
            WebServiceClient client = env.serviceClient();
            DemoEntity entity = new DemoEntity();
            DemoEntityServiceResponse response = client.put(SERVICE_CREATE,
                    DemoEntityServiceResponse.class,
                    entity,
                    null,
                    FileUtils.MIME_TYPE_JSON);
            assertNotNull(response);
            assertNotNull(response.getEntity());
            entity = response.getEntity();
            Map<String, String> params = Map.of("key", entity.entityKey().getKey());
            response = client.get(SERVICE_GET,
                    DemoEntityServiceResponse.class,
                    params,
                    FileUtils.MIME_TYPE_JSON);
            assertNotNull(response);
            assertNotNull(response.getEntity());
            entity = response.getEntity();
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void post() {
        try {
            WebServiceClient client = env.serviceClient();
            DemoEntity entity = new DemoEntity();
            DemoEntityServiceResponse response = client.put(SERVICE_CREATE,
                    DemoEntityServiceResponse.class,
                    entity,
                    null,
                    FileUtils.MIME_TYPE_JSON);
            assertNotNull(response);
            assertNotNull(response.getEntity());
            entity = response.getEntity();
            String name = "updated-entity";
            entity.setName(name);
            response = client.post(SERVICE_UPDATE,
                    DemoEntityServiceResponse.class,
                    entity,
                    null,
                    FileUtils.MIME_TYPE_JSON);
            assertNotNull(response);
            assertNotNull(response.getEntity());
            entity = response.getEntity();
            assertEquals(name, entity.getName());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void put() {
        try {
            WebServiceClient client = env.serviceClient();
            DemoEntity entity = new DemoEntity();
            DemoEntityServiceResponse response = client.put(SERVICE_CREATE,
                    DemoEntityServiceResponse.class,
                    entity,
                    null,
                    FileUtils.MIME_TYPE_JSON);
            assertNotNull(response);
            assertNotNull(response.getEntity());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void delete() {
        try {
            WebServiceClient client = env.serviceClient();
            DemoEntity entity = new DemoEntity();
            DemoEntityServiceResponse response = client.put(SERVICE_CREATE,
                    DemoEntityServiceResponse.class,
                    entity,
                    null,
                    FileUtils.MIME_TYPE_JSON);
            assertNotNull(response);
            assertNotNull(response.getEntity());
            entity = response.getEntity();
            List<String> params = List.of(entity.entityKey().getKey());
            BooleanServiceResponse r = client.delete(SERVICE_DELETE,
                    BooleanServiceResponse.class,
                    params,
                    FileUtils.MIME_TYPE_JSON);
            assertNotNull(response);
            assertNotNull(response.getEntity());
            Boolean result = r.getEntity();
            assertTrue(result);
            params = List.of(UUID.randomUUID().toString());
            r = client.delete(SERVICE_DELETE,
                    BooleanServiceResponse.class,
                    params,
                    FileUtils.MIME_TYPE_JSON);
            result = r.getEntity();
            assertFalse(result);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}