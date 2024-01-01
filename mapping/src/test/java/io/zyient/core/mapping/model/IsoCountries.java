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

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "tb_iso_country")
public class IsoCountries implements IEntity<IsoCountryKey> {
    @EmbeddedId
    private IsoCountryKey key;
    @Column(name = "alpha_2")
    private String alpha2;
    @Column(name = "alpha_3")
    private String alpha3;
    @Column(name = "country_code")
    private String countryCode;
    @Column(name = "iso_3166_2")
    private String iso3166Code;
    @Column(name = "region")
    private String region;
    @Column(name = "sub_region")
    private String subRegion;
    @Column(name = "intermediate_region")
    private String intermediateRegion;
    @Column(name = "region_code")
    private String regionCode;
    @Column(name = "sub_region_code")
    private String subRegionCode;
    @Column(name = "intermediate_region_code")
    private String intermediateRegionCode;

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(IsoCountryKey key) {
        return this.key.compareTo(key);
    }

    /**
     * Copy the changes from the specified source entity
     * to this instance.
     * <p>
     * All properties other than the Key will be copied.
     * Copy Type:
     * Primitive - Copy
     * String - Copy
     * Enum - Copy
     * Nested Entity - Copy Recursive
     * Other Objects - Copy Reference.
     *
     * @param source  - Source instance to Copy from.
     * @param context - Execution context.
     * @return - Copied Entity instance.
     * @throws CopyException
     */
    @Override
    public IEntity<IsoCountryKey> copyChanges(IEntity<IsoCountryKey> source, Context context) throws CopyException {
        return null;
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<IsoCountryKey> clone(Context context) throws CopyException {
        return null;
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public IsoCountryKey entityKey() {
        return key;
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {

    }
}
