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
import io.zyient.base.common.utils.CollectionUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class Section extends Cell<String> {
    public static final String __PREFIX = "section.";
    private List<Cell<?>> blocks;

    public Section() {
        super();
    }

    public Section(@NonNull String parentId, int index) {
        super(parentId, index);
    }

    @Override
    protected Cell<?> find(String parentId, @NonNull String[] paths, int index) {
        if (checkId(parentId, paths, index)) {
            if (index == paths.length - 1) {
                return this;
            } else if (blocks != null) {
                for (Cell<?> block : blocks) {
                    Cell<?> ret = block.find(getId(), paths, index + 1);
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

    public <T> Cell<T> add(@NonNull Class<? extends Cell<T>> type,
                           int index) throws Exception {
        if (blocks == null) {
            blocks = new ArrayList<>();
        }
        Cell<T> cell = type.getDeclaredConstructor(String.class, Integer.class)
                .newInstance(getId(), index);
        CollectionUtils.setAtIndex(blocks, index, cell);
        return cell;
    }
}
