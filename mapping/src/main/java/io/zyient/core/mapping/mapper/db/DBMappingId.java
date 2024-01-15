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

package io.zyient.core.mapping.mapper.db;

import com.google.common.base.Strings;
import io.zyient.base.common.model.entity.IKey;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@Embeddable
public class DBMappingId implements IKey {
    @Column(name = "condition_id")
    private String conditionId;
    @Column(name = "sequence")
    private int sequence;

    /**
     * Get the String representation of the key.
     *
     * @return - Key String
     */
    @Override
    public String stringKey() {
        return String.format("%s::%d", conditionId, sequence);
    }

    /**
     * Compare the current key to the target.
     *
     * @param key - Key to compare to
     * @return - == 0, < -x, > +x
     */
    @Override
    public int compareTo(IKey key) {
        if (key instanceof DBMappingId) {
            int ret = conditionId.compareTo(((DBMappingId) key).conditionId);
            if (ret == 0) {
                ret = sequence - ((DBMappingId) key).sequence;
            }
            return ret;
        }
        throw new RuntimeException(String.format("Invalid key type. [type=%s]", key.getClass().getCanonicalName()));
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
        String[] parts = value.split("::");
        if (parts.length != 2) {
            throw new Exception(String.format("Invalid key: [key=%s]", value));
        }
        conditionId = parts[0];
        if (Strings.isNullOrEmpty(conditionId)) {
            throw new Exception(String.format("Invalid key: condition id is NULL/empty. [key=%s]", value));
        }
        sequence = Integer.parseInt(parts[1].trim());
        return this;
    }
}
