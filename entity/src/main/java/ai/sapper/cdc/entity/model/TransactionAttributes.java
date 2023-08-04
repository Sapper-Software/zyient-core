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

package ai.sapper.cdc.entity.model;

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class TransactionAttributes {
    public static final String KEY_TNX_START = "transaction.start";
    public static final String KEY_TNX_END = "transaction.end";

    private final Map<String, String> attributes = new HashMap<>();

    public TransactionAttributes start(@NonNull String tnxId) {
        attributes.put(KEY_TNX_START, tnxId);
        return this;
    }

    public String start() {
        return attributes.get(KEY_TNX_START);
    }

    public TransactionAttributes end(@NonNull String tnxId) {
        attributes.put(KEY_TNX_END, tnxId);
        return this;
    }

    public String end() {
        return attributes.get(KEY_TNX_END);
    }

    protected TransactionAttributes add(@NonNull String key, @NonNull String value) {
        attributes.put(key, value);
        return this;
    }

    protected String get(@NonNull String key) {
        return attributes.get(key);
    }

    protected boolean remove(@NonNull String key) {
        return attributes.remove(key) != null;
    }
}
