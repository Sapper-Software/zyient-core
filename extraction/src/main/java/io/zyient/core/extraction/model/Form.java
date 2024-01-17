/*
 * Copyright(C) (2024) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.core.extraction.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class Form extends Cell<String> {
    public static final String __PREFIX = "form.";

    private Map<String, FormField<?>> fields;

    public Form() {
        super();
    }

    public Form(@NonNull String parentId, int index) {
        super(parentId, index);
    }

    @Override
    protected Cell<?> find(String parentId, @NonNull String[] paths, int index) {
        if (checkId(parentId, paths, index)) {
            if (index == paths.length - 1) {
                return this;
            } else if (fields != null) {
                for (String key : fields.keySet()) {
                    FormField<?> field = fields.get(key);
                    Cell<?> ret = field.find(getId(), paths, index + 1);
                    if (ret != null) {
                        return ret;
                    }
                }
            }
        }
        return null;
    }

    @Override
    protected String parseId(int index) {
        return String.format("%s%d", __PREFIX, index);
    }

    public <V> FormField<V> add(@NonNull Class<? extends Cell<V>> valueType,
                                int index) throws Exception {
        if (fields == null) {
            fields = new HashMap<>();
        }
        FormField<V> field = new FormField<>(getId(), index);
        field.createLabelCell();
        field.createValueCell(valueType);
        fields.put(field.getId(), field);
        return field;
    }
}
