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

package io.zyient.core.mapping.model.mapping;

import io.zyient.base.common.config.Config;
import io.zyient.base.common.utils.CollectionUtils;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

@Getter
@Setter
public class NestedMappedElement extends MappedElement {
    @Config(name = "target.collection", required = false, type = CollectionType.class)
    private CollectionType collectionType = CollectionType.List;
    @Config(name = "mapping.name")
    private String mapping;
    @Config(name = "mapping.file")
    private String mappingDef;
    @Config(name = "terminateOnValidationErrors", required = false, type = Boolean.class)
    private boolean terminateOnValidationErrors = false;

    public Class<?> parseCollectionType() throws Exception {
        switch (collectionType) {
            case Set -> {
                return Set.class;
            }
            case List -> {
                return List.class;
            }
            case Linked -> {
                return LinkedList.class;
            }
        }
        throw new Exception(String.format("Collection type [%s] not supported", collectionType.name()));
    }

    public Object createCollectionType() throws Exception {
        switch (collectionType) {
            case Set -> {
                return CollectionUtils.createSet(getTargetType());
            }
            case List -> {
                return CollectionUtils.createList(getTargetType());
            }
            case Linked -> {
                return CollectionUtils.createLinkedList(getTargetType());
            }
        }
        throw new Exception(String.format("Collection type [%s] not supported", collectionType.name()));
    }
}
