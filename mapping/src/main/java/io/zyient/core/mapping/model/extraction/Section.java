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

package io.zyient.core.mapping.model.extraction;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
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
    private List<Cell<?>> blocks;

    public Section() {
        super();
    }

    public Section(@NonNull String parentId, int index) {
        super(parentId, index);
    }

    public <T> Cell<T> add(@NonNull Class<? extends Cell<T>> type) throws Exception {
        if (blocks == null) {
            blocks = new ArrayList<>();
        }
        Cell<T> cell = type.getDeclaredConstructor(String.class, Integer.class)
                .newInstance(getId(), blocks.size());
        blocks.add(cell);
        return cell;
    }
}
