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

package ai.sapper.cdc.core.model;

import ai.sapper.cdc.common.model.ValidationExceptions;
import ai.sapper.cdc.common.model.entity.EEntityState;
import ai.sapper.cdc.common.model.entity.IEntity;
import ai.sapper.cdc.common.model.entity.IKey;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;

@Getter
@Setter
@MappedSuperclass
public abstract class BaseEntity<K extends IKey> implements IEntity<K> {
    @Setter(AccessLevel.NONE)
    @Transient
    private final EntityState state = new EntityState();

    public BaseEntity() {
        state.setState(EEntityState.Syncing);
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public final void validate() throws ValidationExceptions {
        try {
            doValidate();
        } catch (Exception ex) {
            state.error(ex);
        }
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    public abstract void doValidate() throws ValidationExceptions;
}