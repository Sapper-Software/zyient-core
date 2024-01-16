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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class Cell<T> {
    private String id;
    private String parentId;
    private int index;
    private BoundingBox boundingBox;
    private T data;
    private Map<String, Tag> tags;
    private double confidence;

    public Cell() {
    }

    public Cell(@NonNull String parentId, int index) {
        id = String.format("%s.%d", parentId, index);
        this.index = index;
    }

    public Tag addTag(@NonNull String label, double confidence) {
        Preconditions.checkArgument(confidence >= 0 && confidence <= 1);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(label));
        Tag tag = new Tag();
        tag.setLabel(label);
        tag.setConfidence(confidence);
        tags.put(tag.getLabel(), tag);

        return tag;
    }
}
