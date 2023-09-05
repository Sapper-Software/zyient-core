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

package io.zyient.base.core.messaging.aws;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import lombok.NonNull;

public class AwsSQSContext extends Context {
    public static final String KEY_QUEUE = "queue";

    public AwsSQSContext() {

    }

    public AwsSQSContext(@NonNull String queue) {
        put(KEY_QUEUE, queue);
    }

    public String getQueue() {
        Object v = get(KEY_QUEUE);
        if (v instanceof String) {
            return (String) v;
        }
        return null;
    }

    public AwsSQSContext setQueue(@NonNull String queue) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(queue));
        put(KEY_QUEUE, queue);
        return this;
    }
}
