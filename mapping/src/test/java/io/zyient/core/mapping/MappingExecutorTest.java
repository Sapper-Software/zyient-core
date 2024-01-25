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

package io.zyient.core.mapping;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.core.mapping.env.DemoDataStoreEnv;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.ReadCompleteCallback;
import io.zyient.core.mapping.readers.ReadResponse;
import lombok.NonNull;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.fail;

class MappingExecutorTest {
    private static final String __CONFIG_FILE = "src/test/resources/mapping/test-mapping-env.xml";
    private static final String __CONFIG_FILE_MAPPING = "src/test/resources/mapping/mapping.xml";
    private static final String __INPUT_CUSTOMER_CSV = "src/test/resources/data/customers_202311231439.csv";

    private static XMLConfiguration xmlConfiguration = null;
    private static DemoDataStoreEnv env = new DemoDataStoreEnv();

    @BeforeAll
    static void beforeAll() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.create(xmlConfiguration);
        env.connectionManager().save();

        XMLConfiguration mConfig = ConfigReader.readFromFile(__CONFIG_FILE_MAPPING);
        MappingExecutor.create(mConfig, env);
    }

    @AfterAll
    static void afterAll() throws Exception {
        env.close();
    }

    @Test
    void read() {
        try {
            File input = new File(__INPUT_CUSTOMER_CSV);
            InputContentInfo ci = new InputContentInfo();
            Callback callback = new Callback();
            ci.path(input)
                    .sourceURI(input.toURI())
                    .callback(callback)
                    .documentId(UUID.randomUUID().toString());
            ci.put("country", "India");
            MappingExecutor.defaultInstance().read(ci);
            while (!callback.finished) {
                Thread.sleep(1000);
            }
            if (callback.error != null) {
                fail(callback.error);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    public static class Callback implements ReadCompleteCallback {
        private boolean finished = false;
        private Throwable error;

        @Override
        public void onError(@NonNull InputContentInfo contentInfo, @NonNull Throwable error) {
            DefaultLogger.error("Read pipeline failed.", error);
            this.error = error;
            finished = true;
        }

        @Override
        public void onSuccess(@NonNull InputContentInfo contentInfo, @NonNull ReadResponse response) {
            try {
                String jc = JSONUtils.asString(contentInfo);
                DefaultLogger.info(String.format("INPUT=[%s]", jc));
                finished = true;
            } catch (Exception ex) {
                DefaultLogger.stacktrace(ex);
                error = ex;
                finished = true;
            }
        }
    }
}