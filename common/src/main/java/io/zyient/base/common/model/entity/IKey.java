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

package io.zyient.base.common.model.entity;

import lombok.NonNull;

import java.io.Serializable;

/**
 * Define an entity Key type.
 */
public interface IKey extends Serializable {
    String __DEFAULT_SEPARATOR = "::";

    /**
     * Get the String representation of the key.
     *
     * @return - Key String
     */
    String stringKey();

    /**
     * Compare the current key to the target.
     *
     * @param key - Key to compare to
     * @return - == 0, < -x, > +x
     */
    int compareTo(IKey key);

    /**
     * Parse this key type from the input string.
     *
     * @param value - Input key string.
     * @return - this
     * @throws Exception
     */
    IKey fromString(@NonNull String value) throws Exception;
}