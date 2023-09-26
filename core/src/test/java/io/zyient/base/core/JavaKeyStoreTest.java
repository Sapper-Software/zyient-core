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

package io.zyient.base.core;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.keystore.JavaKeyStore;
import io.zyient.base.core.keystore.KeyStore;
import io.zyient.base.core.utils.DemoEnv;
import io.zyient.base.core.utils.JavaKeyStoreUtil;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class JavaKeyStoreTest {
    private static final String __CONFIG_FILE = "src/test/resources/keystore.xml";
    private static final String __CONFIG_PATH = "config";
    private static XMLConfiguration xmlConfiguration = null;
    private static DemoEnv env = new DemoEnv();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.init(xmlConfiguration);
    }

    @Test
    void read() {
        try {
            String keyName = "oracle-demo-password";
            String keyValue = "test1234";
            String password = "test1234";

            JavaKeyStoreUtil util = new JavaKeyStoreUtil();
            util.setConfigFile(__CONFIG_FILE);
            util.setPassword(password);
            util.setKey(keyName);
            util.setValue(keyValue);
            util.run();

            util.setKey(UUID.randomUUID().toString());
            util.setValue("Dummy");
            util.run();

            HierarchicalConfiguration<ImmutableNode> configNode = xmlConfiguration.configurationAt("");
            KeyStore store = new JavaKeyStore().withPassword(password);
            store.init(configNode, env);

            store.save(keyName, keyValue);
            store.flush();
            String v = store.read(keyName);
            assertEquals(keyValue, v);

            File file = ((JavaKeyStore)store).filePath(password);
            assertNotNull(file);
            //store.delete();
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }
}