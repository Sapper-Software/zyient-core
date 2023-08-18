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

import ai.sapper.cdc.common.utils.ChecksumUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Getter
@Setter
@Embeddable
public class StringKey implements IKey, Serializable {
    @Column(name = "key")
    private String key;

    public StringKey() {
    }

    public StringKey(@NonNull String key) {
        this.key = key;
    }

    /**
     * Get the String representation of the key.
     *
     * @return - Key String
     */
    @Override
    public String stringKey() {
        return key;
    }

    /**
     * Compare the current key to the target.
     *
     * @param key - Key to compare to
     * @return - == 0, < -x, > +x
     */
    @Override
    public int compareTo(IKey key) {
        if (key instanceof StringKey) {
            StringKey s = (StringKey) key;
            return this.key.compareTo(s.key);
        }
        return -1;
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