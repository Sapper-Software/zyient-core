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

import dev.morphia.annotations.Entity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.model.entity.NativeKey;
import jakarta.persistence.Embeddable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Embeddable
@Entity
public class LongKey extends NativeKey<Long> {
    public LongKey() {
        super(Long.class);
    }

    /**
     * Compare the current key to the target.
     *
     * @param key - Key to compare to
     * @return - == 0, < -x, > +x
     */
    @Override
    public int compareTo(IKey key) {
        if (key instanceof LongKey) {
            return (int) (getKey() - ((LongKey) key).getKey());
        }
        return -1;
    }
}
