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

package io.zyient.base.core.connections.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.Settings;
import io.zyient.base.common.messaging.MessagingError;
import io.zyient.base.common.utils.DefaultLogger;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public class KafkaAdminHelper implements Closeable {

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class KafkaTopic {
        public static final String CONFIG_NAME = "name";
        public static final String CONFIG_REPLICAS = "replicas";
        public static final String CONFIG_MIN_ISR = "minIsr";
        public static final String CONFIG_PARTITIONS = "partitions";

        private String name;
        private short replicas = 1;
        private short minIsr = 1;
        private int partitions = 1;
        private Map<String, String> config = null;

        public KafkaTopic() {
        }

        public KafkaTopic(@NonNull KafkaTopic source) {
            this.name = source.name;
            this.replicas = source.replicas;
            this.partitions = source.partitions;
            this.minIsr = source.minIsr;
            if (source.config != null) {
                this.config = new HashMap<>(source.config);
            }
        }
    }

    private AdminClient kafkaAdmin;
    private KafkaAdminConfig adminConfig;

    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws MessagingError {
        try {
            adminConfig = new KafkaAdminConfig(config);
            adminConfig.read();
            KafkaAdminSettings settings = (KafkaAdminSettings) adminConfig.settings();

            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers);

            kafkaAdmin = AdminClient.create(props);
        } catch (Exception e) {
            DefaultLogger.error(e.getLocalizedMessage());
            DefaultLogger.stacktrace(e);
            throw new MessagingError(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (kafkaAdmin != null) {
            kafkaAdmin.close();
            kafkaAdmin = null;
        }
    }

    public void createTopic(@NonNull KafkaTopic topic) throws MessagingError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic.name));
        Preconditions.checkNotNull(kafkaAdmin);
        if (topic.partitions <= 0) topic.partitions = 1;
        if (topic.replicas <= 0) topic.replicas = 1;

        try {
            NewTopic newTopic = new NewTopic(topic.name, topic.partitions, topic.replicas);

            if (topic.config != null) {
                newTopic.configs(topic.config);
            }
            CreateTopicsResult result = kafkaAdmin.createTopics(
                    Collections.singleton(newTopic)
            );
            KafkaFuture<Void> future = result.values().get(topic.name);
            future.get();

            DefaultLogger.info(String.format("Created new Kafka Topic. [name=%s]", topic.name));
        } catch (Exception e) {
            throw new MessagingError(String.format("Error creating topic. [name=%s]", topic.name), e);
        }
    }


    public void deleteTopic(@NonNull String name) throws MessagingError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkNotNull(kafkaAdmin);
        try {
            DeleteTopicsResult result = kafkaAdmin.deleteTopics(Collections.singleton(name));
            KafkaFuture<Void> future = result.topicNameValues().get(name);
            future.get();

            DefaultLogger.info(String.format("Deleted Kafka Topic. [name=%s]", name));
        } catch (Exception e) {
            throw new MessagingError(String.format("Error deleting topic. [name=%s]", name), e);
        }
    }

    public List<KafkaTopic> list() throws MessagingError {
        Preconditions.checkNotNull(kafkaAdmin);
        try {
            ListTopicsResult result = kafkaAdmin.listTopics();
            KafkaFuture<Collection<TopicListing>> future = result.listings();
            Collection<TopicListing> listings = future.get();
            if (listings != null && !listings.isEmpty()) {
                List<KafkaTopic> topics = new ArrayList<>(listings.size());
                for (TopicListing tl : listings) {
                    if (!tl.isInternal()) {
                        TopicDescription desc = get(tl.name());
                        if (desc != null) {
                            KafkaTopic kt = new KafkaTopic();
                            kt.name = desc.name();
                            if (desc.partitions() != null) {
                                kt.partitions = desc.partitions().size();
                            }
                            topics.add(kt);
                        }
                    }
                }
                if (!topics.isEmpty()) return topics;
            }
        } catch (Exception e) {
            throw new MessagingError("Error listing topics.", e);
        }
        return null;
    }

    public List<KafkaTopic> search(@NonNull String regex) throws MessagingError {
        Preconditions.checkNotNull(kafkaAdmin);
        try {
            List<KafkaTopic> all = list();
            if (all != null && !all.isEmpty()) {
                Pattern pattern = Pattern.compile(regex);
                List<KafkaTopic> result = new ArrayList<>();
                for (KafkaTopic kt : all) {
                    Matcher m = pattern.matcher(kt.name);
                    if (m.matches()) {
                        result.add(kt);
                    }
                }
                if (!result.isEmpty()) return result;
            }
        } catch (Exception e) {
            throw new MessagingError("Error listing topics.", e);
        }
        return null;
    }

    public TopicDescription get(@NonNull String name) throws MessagingError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkNotNull(kafkaAdmin);
        try {
            DescribeTopicsResult result = kafkaAdmin.describeTopics(List.of(name));
            KafkaFuture<TopicDescription> future = result.topicNameValues().get(name);

            TopicDescription desc = future.get();
            if (desc != null) {
                return desc;
            }
        } catch (UnknownTopicOrPartitionException une) {
            // Do nothing...
        } catch (Exception e) {
            if (!checkNotFoundError(e)) {
                throw new MessagingError(String.format("Error checking for topic. [name=%s]", name), e);
            }
        }
        return null;
    }

    public boolean exists(@NonNull String name) throws MessagingError {
        return (get(name) != null);
    }

    private boolean checkNotFoundError(Throwable t) {
        Throwable c = t;
        while (c != null) {
            if (c instanceof UnknownTopicOrPartitionException) return true;
            c = c.getCause();
        }
        return false;
    }

    @Getter
    @Setter
    public static class KafkaAdminSettings extends Settings {
        public static final String CONFIG_BOOTSTRAP = "servers";

        @Config(name = CONFIG_BOOTSTRAP)
        private String bootstrapServers;
    }

    public static class KafkaAdminConfig extends ConfigReader {
        public static final String __CONFIG_PATH = "admin";

        public KafkaAdminConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, KafkaAdminSettings.class);
        }
    }
}
