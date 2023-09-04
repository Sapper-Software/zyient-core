/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.base.core.stores.impl;

import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.stores.BaseSearchResult;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;

@Getter
@Setter
public class EntitySearchResult<T extends IEntity<?>> extends BaseSearchResult<T> {
    private Collection<T> entities;

    public EntitySearchResult(@Nonnull Class<? extends IEntity<?>> type) {
        super(type);
    }

    public void add(@Nonnull T entity) {
        if (entities == null) {
            entities = new ArrayList<>();
        }
        entities.add(entity);
    }
}
