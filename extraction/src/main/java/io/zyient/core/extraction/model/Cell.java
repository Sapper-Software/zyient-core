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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.opencv.core.Scalar;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class Cell<T> {
    public static final String SEPARATOR = "/";

    private String id;
    private String parentId;
    private int index;
    private BoundingBox boundingBox;
    private T data;
    private Map<String, Tag> tags;
    private float confidence;
    private Scalar background;
    private Scalar textColor;
    private FontInfo fontInfo;

    public Cell() {
    }

    public Cell(@NonNull String parentId, int index) {
        id = String.format("%s%s%s", parentId, SEPARATOR, parseId(index));
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

    protected boolean checkId(String parentId, @NonNull String[] paths, int index) {
        String key = paths[index];
        if (!Strings.isNullOrEmpty(parentId)) {
            key = String.format("%s%s%s", parentId, SEPARATOR, key);
        }
        return id.compareTo(key) == 0;
    }

    protected abstract Cell<?> find(String parentId, @NonNull String[] paths, int index);

    protected abstract String parseId(int index);
}
