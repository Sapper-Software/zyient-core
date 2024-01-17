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
public class Source {
    private String referenceId;
    private String sourceURI;
    private List<DocumentSection> documents;
    private Context metadata;

    public Source() {
        super();
    }

    public Source(@NonNull String referenceId,
                  @NonNull String sourceURI) {
        this.referenceId = referenceId;
        this.sourceURI = sourceURI;
    }

    public Context addMetadata(@NonNull String name,
                               @NonNull Object value) {
        if (metadata == null) {
            metadata = new Context();
        }
        metadata.put(name, value);
        return metadata;
    }

    public DocumentSection create(int index) {
        if (documents == null) {
            documents = new ArrayList<>();
        }
        DocumentSection doc = new DocumentSection(index);
        CollectionUtils.setAtIndex(documents, index, doc);
        return doc;
    }


    public Cell<?> find(@NonNull String path) {
        if (documents != null) {
            String[] paths = path.split("/");
            for (DocumentSection doc : documents) {
                Cell<?> ret = doc.find(null, paths, 0);
                if (ret != null) {
                    return ret;
                }
            }
        }
        return null;
    }
}
