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

package io.zyient.base.core.messaging.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import lombok.NonNull;

public class KafkaContext extends Context {
    public static final String KEY_TOPIC = "topic";
    public static final String KEY_PARTITION = "partition";

    public KafkaContext() {
    }

    public KafkaContext(@NonNull String topic, int partition) {
        setTopic(topic);
        setPartition(partition);
    }

    public KafkaContext setTopic(@NonNull String topic) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));
        put(KEY_TOPIC, topic);
        return this;
    }

    public KafkaContext setPartition(int partition) {
        Preconditions.checkArgument(partition >= 0);
        put(KEY_PARTITION, partition);
        return this;
    }

    public String getTopic() {
        Object v = get(KEY_TOPIC);
        if (v != null) {
            return (String) v;
        }
        return null;
    }

    public int getPartition() {
        Object v = get(KEY_PARTITION);
        if (v != null) {
            return (Integer) v;
        }
        return -1;
    }
}
