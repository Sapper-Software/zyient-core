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

package io.zyient.base.common.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public class Context {
    public Map<String, Object> params = new HashMap<>();

    public Context() {
    }

    public Context(@NonNull Context source) {
        params.putAll(source.params);
    }

    public Context(@NonNull Map<String, Object> params) {
        this.params.putAll(params);
    }

    public Context put(@NonNull String key, Object value) {
        params.put(key, value);
        return this;
    }

    @JsonIgnore
    public Object get(@NonNull String key) {
        return params.get(key);
    }

    public boolean containsKey(@NonNull String key) {
        return params.containsKey(key);
    }

    public boolean remove(@NonNull String key) {
        return (params.remove(key) != null);
    }

    public void clear() {
        params.clear();
    }

    @JsonIgnore
    public boolean isEmpty() {
        return params.isEmpty();
    }
}
