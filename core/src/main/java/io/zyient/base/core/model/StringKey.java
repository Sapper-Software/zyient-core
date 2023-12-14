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

package io.zyient.base.core.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dev.morphia.annotations.Entity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.model.entity.NativeKey;
import io.zyient.base.common.utils.ChecksumUtils;
import jakarta.persistence.Embeddable;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@Embeddable
@Entity
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class StringKey extends NativeKey<String> {

    public StringKey() {
        super(String.class);
    }

    public StringKey(@NonNull String key) {
        super(String.class);
        setKey(key);
    }

    /**
     * Compare the current key to the target.
     *
     * @param key - Key to compare to
     * @return - == 0, < -x, > +x
     */
    @Override
    public int compareTo(IKey key) {
        if (key instanceof StringKey s) {
            return this.getKey().compareTo(s.getKey());
        }
        return -1;
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
        setKey(value);
        return this;
    }

    @Override
    public int hashCode() {
        return ChecksumUtils.getHashCode(stringKey());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StringKey) {
            return compareTo((IKey) obj) == 0;
        }
        return super.equals(obj);
    }
}