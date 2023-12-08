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

package io.zyient.core.messaging.azure;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.messaging.MessageObject;
import io.zyient.core.messaging.azure.builders.AzureMessageConsumerBuilder;
import io.zyient.core.messaging.azure.builders.AzureMessageProducerBuilder;
import io.zyient.core.messaging.azure.builders.DemoAzureMessageConsumerBuilder;
import io.zyient.core.messaging.azure.builders.DemoAzureMessageProducerBuilder;
import io.zyient.core.messaging.chronicle.BaseChronicleMessage;
import io.zyient.core.messaging.env.DemoEnv;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BaseAzureConsumerTest {
    private static final String __CONFIG_FILE = "src/test/resources/azure/azure-queue-test.xml";
    private static final String __PRODUCER_NAME = "test-queue-producer";
    private static final String __CONSUMER_NAME = "test-queue-consumer";
    private static final String __MESSAGE_FILE = "src/test/resources/test-message.xml";
    private static XMLConfiguration xmlConfiguration = null;
    private static DemoEnv env;
    private static final String CONFIG_PRODUCER_PATH = "demo.producer";
    private static final String CONFIG_CONSUMER_PATH = "demo.consumer";
    private static DemoAzureMessageProducer producer;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.readFromFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
        env = new DemoEnv();
        env.init(xmlConfiguration);
        env.connectionManager().save();
        HierarchicalConfiguration<ImmutableNode> node = env.baseConfig();
        initProducer(node);
    }

    private static void initProducer(HierarchicalConfiguration<ImmutableNode> node) throws Exception {
        HierarchicalConfiguration<ImmutableNode> pn = node.configurationAt(CONFIG_PRODUCER_PATH);
        AzureMessageProducerBuilder<String> builder
                = new DemoAzureMessageProducerBuilder();
        producer = (DemoAzureMessageProducer) builder.withEnv(env).build(pn);
    }

    private static DemoAzureMessageConsumer initConsumer(HierarchicalConfiguration<ImmutableNode> node) throws Exception {
        HierarchicalConfiguration<ImmutableNode> pn = node.configurationAt(CONFIG_CONSUMER_PATH);
        AzureMessageConsumerBuilder<String> builder
                = new DemoAzureMessageConsumerBuilder();
        return (DemoAzureMessageConsumer) builder.withEnv(env).build(pn);
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
            File mf = new File(__MESSAGE_FILE);
            assertTrue(mf.exists());
            StringBuilder text = new StringBuilder();
            FileReader fr = new FileReader(mf);   //reads the file
            try (BufferedReader reader = new BufferedReader(fr)) {  //creates a buffering character input stream
                String line;
                while ((line = reader.readLine()) != null) {
                    text.append(line).append("\n");
                }
            }
            int mCount = 10;
            Thread thread = new Thread(new ConsumerThread(mCount));
            thread.start();

            String cid = "FIRST";
            for (int ii = 0; ii < mCount; ii++) {
                BaseChronicleMessage<String> m = new BaseChronicleMessage<>();
                m.key(String.format("TEST-MESSAGE-%d", ii));
                m.value(text.toString());
                m.correlationId(cid);
                m.mode(MessageObject.MessageMode.New);
                producer.send(m);
                cid = m.id();
            }
            thread.join();
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }

    @Test
    void send() {
        try {
            File mf = new File(__MESSAGE_FILE);
            assertTrue(mf.exists());
            StringBuilder text = new StringBuilder();
            FileReader fr = new FileReader(mf);   //reads the file
            try (BufferedReader reader = new BufferedReader(fr)) {  //creates a buffering character input stream
                String line;
                while ((line = reader.readLine()) != null) {
                    text.append(line).append("\n");
                }
            }
            int mCount = 10;
            String cid = "FIRST";
            for (int ii = 0; ii < mCount; ii++) {
                BaseChronicleMessage<String> m = new BaseChronicleMessage<>();
                m.key(String.format("TEST-MESSAGE-%d", ii));
                m.value(text.toString());
                m.correlationId(cid);
                m.mode(MessageObject.MessageMode.New);
                producer.send(m);
                cid = m.id();
            }

            long index = -1;
            int count = 0;
            int retry = 0;
            try (DemoAzureMessageConsumer consumer = initConsumer(env.baseConfig())) {
                while (true) {
                    List<MessageObject<String, String>> messages = consumer.nextBatch();
                    if (messages != null && !messages.isEmpty()) {
                        retry = 0;
                        for (MessageObject<String, String> m : messages) {
                            consumer.ack(m.id(), false);
                            AzureMessage<String> cm = (AzureMessage<String>) m;
                            count++;
                        }
                        consumer.commit();
                    } else if (retry > 10) {
                        break;
                    } else {
                        retry++;
                        Thread.sleep(100);
                    }
                }
                DefaultLogger.info(String.format("Received message count = %d", count));
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }

    public static class ConsumerThread implements Runnable {
        private DemoAzureMessageConsumer consumer;
        private final int mCount;

        public ConsumerThread(int mCount) throws Exception {
            this.mCount = mCount;
        }

        @Override
        public void run() {
            try {
                consumer = initConsumer(env.baseConfig());
                int count = 0;
                int retry = 0;
                while (true) {
                    List<MessageObject<String, String>> messages = consumer.nextBatch();
                    if (messages != null && !messages.isEmpty()) {
                        retry = 0;
                        for (MessageObject<String, String> m : messages) {
                            consumer.ack(m.id(), false);
                            count++;
                        }
                        consumer.commit();
                    } else if (retry > 10) {
                        break;
                    } else {
                        retry++;
                        Thread.sleep(100);
                    }
                }
                DefaultLogger.info(String.format("Received message count = %d", count));
                assertEquals(mCount, count);
            } catch (Throwable t) {
                DefaultLogger.stacktrace(t);
                DefaultLogger.error("Consumer thread terminated.", t);
            } finally {
                try {
                    consumer.close();
                } catch (Exception ex) {
                    DefaultLogger.stacktrace(ex);
                }
            }
        }
    }
}