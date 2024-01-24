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

package io.zyient.base.core.connections.ws;

import com.google.common.base.Preconditions;
import io.zyient.base.common.utils.ChecksumUtils;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.TestUtils;
import io.zyient.base.core.model.StringKey;
import io.zyient.base.core.services.model.FileEntity;
import io.zyient.base.core.services.model.FileEntityServiceResponse;
import io.zyient.base.core.services.test.WSTestEnv;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class WebServiceClientFileTest {
    public static final String __CONFIG_FILE = "src/test/resources/ws/ws-client-test.xml";
    public static final String __TEST_FILE = "src/test/resources/data/search/sample-docs.zip";
    public static final String PARAM_KEY_FILE = "file";
    public static final String PARAM_KEY_ENTITY = "entity";
    public static final String SERVICE_UPLOAD = "upload";
    public static final String SERVICE_DOWNLOAD = "download";

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
    void upload() {
        try {
            WebServiceClient client = env.serviceClient();
            File file = new File(__TEST_FILE);
            FileEntity fe = new FileEntity();
            fe.setKey(new StringKey(UUID.randomUUID().toString()));
            fe.setPath(file);
            fe.setCheckSum(ChecksumUtils.computeSHA256(file));
            fe.setName(file.getName());
            FileEntityServiceResponse response = client.upload(SERVICE_UPLOAD,
                    FileEntityServiceResponse.class,
                    fe,
                    null,
                    PARAM_KEY_FILE,
                    PARAM_KEY_ENTITY,
                    file);
            assertNotNull(response);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void download() {
    }
}