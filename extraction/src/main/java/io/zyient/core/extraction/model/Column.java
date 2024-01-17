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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class Column extends Cell<String> {
    public static final String __PREFIX = "column.";

    private List<Cell<?>> values;
    private Map<String, Column> subColumns;

    public Column() {
        super();
    }

    public Column(@NonNull String parentId, int index) {
        super(parentId, index);
    }

    @Override
    protected Cell<?> find(String parentId, @NonNull String[] paths, int index) {
        if (checkId(parentId, paths, index)) {
            if (index == paths.length - 1) {
                return this;
            } else {
                String name = paths[index + 1];
                if (name.startsWith(__PREFIX)) {
                    if (subColumns != null) {
                        for (String key : subColumns.keySet()) {
                            Column column = subColumns.get(key);
                            Cell<?> ret = column.find(getId(), paths, index + 1);
                            if (ret != null) {
                                return ret;
                            }
                        }
                    }
                } else {
                    if (values != null) {
                        for (Cell<?> value : values) {
                            Cell<?> ret = value.find(getId(), paths, index + 1);
                            if (ret != null) {
                                return ret;
                            }
                        }
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

    public <T> Cell<T> addValue(@NonNull Class<? extends Cell<T>> type,
                                int index) throws Exception {
        if (values == null) {
            values = new ArrayList<>();
        }
        if (index < 0) {
            index = values.size();
        }
        Cell<T> cell = type.getDeclaredConstructor(String.class, Integer.class)
                .newInstance(getId(), index);
        if (index > values.size()) {
            for (int ii = values.size(); ii <= index; ii++) {
                values.add(null);
            }
            values.set(index, cell);
        } else {
            values.add(cell);
        }
        return cell;
    }

    public Column addSubColumn(@NonNull String name, int index) {
        if (subColumns == null) {
            subColumns = new HashMap<>();
        }
        Column column = new Column(getId(), index);
        column.setData(name);
        subColumns.put(name, column);
        return column;
    }
}
