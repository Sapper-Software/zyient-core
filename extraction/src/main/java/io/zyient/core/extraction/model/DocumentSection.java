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
import io.zyient.base.common.model.Context;
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
public class DocumentSection extends Cell<String> {
    public static final String __PREFIX = "document.";

    private List<Page> pages;
    private Context metadata;

    public DocumentSection() {
        super();
    }

    @Override
    protected Cell<?> find(String parentId, @NonNull String[] paths, int index) {
        if (checkId(parentId, paths, index)) {
            if (index == paths.length - 1) {
                return this;
            } else if (pages != null) {
                for (Page page : pages) {
                    Cell<?> ret = page.find(getId(), paths, index + 1);
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

    public DocumentSection(int index) {
        Preconditions.checkArgument(index >= 0);
        setParentId(null);
        setId(String.valueOf(index));
    }

    public Page add(int index) {
        if (pages == null) {
            pages = new ArrayList<>();
        }
        Page page = new Page(getId(), index);
        CollectionUtils.setAtIndex(pages, index, page);
        return page;
    }

    public Context addMetadata(@NonNull String name,
                               @NonNull Object value) {
        if (metadata == null) {
            metadata = new Context();
        }
        metadata.put(name, value);
        return metadata;
    }
}
