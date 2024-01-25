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

package io.zyient.core.caseflow.model;

import io.zyient.base.common.model.Context;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;

public class CaseContext extends Context {
    public static final String KEY_CUSTOM_FIELDS = "custom.fields";

    @SuppressWarnings("unchecked")
    public CaseContext addField(@NonNull String name,
                                Object value) {
        Map<String, Object> values = null;
        if (containsKey(KEY_CUSTOM_FIELDS)) {
            values = (Map<String, Object>) get(KEY_CUSTOM_FIELDS);
        } else {
            values = new HashMap<>();
            put(KEY_CUSTOM_FIELDS, values);
        }
        values.put(name, value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> customFields() {
        return (Map<String, Object>) get(KEY_CUSTOM_FIELDS);
    }
}
