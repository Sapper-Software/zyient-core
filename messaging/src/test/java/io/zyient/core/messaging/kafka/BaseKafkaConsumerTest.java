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

package io.zyient.core.messaging.kafka;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.messaging.env.DemoEnv;
import io.zyient.core.messaging.kafka.builders.DemoKafkaConsumerBuilder;
import io.zyient.core.messaging.kafka.builders.DemoKafkaProducerBuilder;
import io.zyient.core.messaging.kafka.builders.KafkaConsumerBuilder;
import io.zyient.core.messaging.kafka.builders.KafkaProducerBuilder;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

class BaseKafkaConsumerTest {
    private static final String __CONFIG_FILE = "src/test/resources/azure/azure-queue-test.xml";
    private static final String __MESSAGE_FILE = "src/test/resources/kafka/producer.properties";
    private static XMLConfiguration xmlConfiguration = null;
    private static DemoEnv env;
    private static final String CONFIG_PRODUCER_PATH = "demo.producer";
    private static final String CONFIG_CONSUMER_PATH = "demo.consumer";
    private static DemoKafkaProducer producer;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.readFromFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
        env = new DemoEnv();
        env.create(xmlConfiguration);
        env.connectionManager().save();
        HierarchicalConfiguration<ImmutableNode> node = env.baseConfig();
        initProducer(node);
    }

    private static void initProducer(HierarchicalConfiguration<ImmutableNode> node) throws Exception {
        HierarchicalConfiguration<ImmutableNode> pn = node.configurationAt(CONFIG_PRODUCER_PATH);
        KafkaProducerBuilder<String> builder = new DemoKafkaProducerBuilder();
        producer = (DemoKafkaProducer) builder.withEnv(env).build(pn);
    }

    private static DemoKafkaConsumer initConsumer(HierarchicalConfiguration<ImmutableNode> node) throws Exception {
        HierarchicalConfiguration<ImmutableNode> pn = node.configurationAt(CONFIG_CONSUMER_PATH);
        KafkaConsumerBuilder<String> builder = new DemoKafkaConsumerBuilder();
        return (DemoKafkaConsumer) builder.withEnv(env).build(pn);
    }

    @AfterAll
    public static void shutdown() throws Exception {
        if (producer != null)
            producer.close();
        if (env != null) {
            env.close();
        }
    }


    @Test
    void nextBatch() {
        try {

        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}