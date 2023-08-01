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

package ai.sapper.cdc.core.messaging.chronicle;

import ai.sapper.cdc.core.connections.TestUtils;
import ai.sapper.cdc.core.utils.DemoEnv;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BaseChronicleConsumerTest {
    private static final String __CONFIG_FILE = "src/test/resources/mapped/chronicle-test.xml";
    private static final String __PRODUCER_NAME = "test-queue-producer";
    private static final String __CONSUMER_NAME = "test-queue-consumer";
    private static final String __MESSAGE_FILE = "src/test/resources/test-message.xml";
    private static XMLConfiguration xmlConfiguration = null;
    private static DemoEnv env;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = TestUtils.readFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
        env = new DemoEnv();
        env.init(xmlConfiguration);
        env.connectionManager().save();
    }

    @AfterAll
    public static void shutdown() throws Exception {
        if (env != null) {
            env.close();
        }
    }

    @Test
    void nextBatch() {
    }

    @Test
    void seek() {
    }
}