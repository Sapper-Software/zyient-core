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

package io.zyient.core.mapping.model;

import io.zyient.base.common.model.entity.IKey;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@Embeddable
public class EntityErrorsId implements IKey {
    @Column(name = "id")
    private String entityId;
    @Column(name = "type")
    private String entityType;

    /**
     * Get the String representation of the key.
     *
     * @return - Key String
     */
    @Override
    public String stringKey() {
        return String.format("%s" + __DEFAULT_SEPARATOR + "%s", entityType, entityId);
    }

    /**
     * Compare the current key to the target.
     *
     * @param key - Key to compare to
     * @return - == 0, < -x, > +x
     */
    @Override
    public int compareTo(IKey key) {
        if (key instanceof EntityErrorsId) {
            int r = entityType.compareTo(((EntityErrorsId) key).entityType);
            if (r == 0) {
                r = entityId.compareTo(((EntityErrorsId) key).entityId);
            }
            return r;
        }
        return Short.MIN_VALUE;
    }

    /**
     * Parse this key type from the input string.
     *
     * @param value - Input key string.
     * @return - this
     * @throws Exception
     */
    @Override
    public IKey fromString(@NonNull String value) throws Exception {
        String[] parts = value.split(__DEFAULT_SEPARATOR);
        if (parts.length != 2) {
            throw new Exception(String.format("Invalid Errors Key: [key=%s]", value));
        }
        entityType = parts[0].trim();
        entityId = parts[1].trim();
        return this;
    }
}
