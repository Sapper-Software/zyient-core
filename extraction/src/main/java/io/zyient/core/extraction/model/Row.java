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

package io.zyient.core.extraction.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class Row extends Section {
    public static final String __PREFIX = "RO.";

    private boolean header;

    public Row() {
        super();
    }

    public Row(@NonNull Table parent, int index) {
        super(parent.getId(), index);
    }

    @Override
    protected Cell<?> find(String parentId, @NonNull String[] paths, int index) {
        if (checkId(parentId, paths, index)) {
            if (getBlocks() != null) {
                for (Cell<?> cell : getBlocks()) {
                    Cell<?> ret = cell.find(getId(), paths, index + 1);
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
}
