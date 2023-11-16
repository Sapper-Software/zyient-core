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

package io.zyient.base.core.stores.impl.rdbms.model;

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.IntegerKey;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

@Getter
@Setter
@Entity
@Table(name = "customers", schema = "test", catalog = "")
public class CustomersEntity implements IEntity<IntegerKey> {
    @EmbeddedId
    private IntegerKey id;
    @Basic
    @Column(name = "customerName")
    private String customerName;
    @Basic
    @Column(name = "contactLastName")
    private String contactLastName;
    @Basic
    @Column(name = "contactFirstName")
    private String contactFirstName;
    @Basic
    @Column(name = "phone")
    private String phone;
    @Basic
    @Column(name = "addressLine1")
    private String addressLine1;
    @Basic
    @Column(name = "addressLine2")
    private String addressLine2;
    @Basic
    @Column(name = "city")
    private String city;
    @Basic
    @Column(name = "state")
    private String state;
    @Basic
    @Column(name = "postalCode")
    private String zipCode;
    @Basic
    @Column(name = "country")
    private String country;
    @Basic
    @Column(name = "salesRepEmployeeNumber")
    private Integer salesRepEmployeeNumber;
    @Basic
    @Column(name = "creditLimit")
    private BigDecimal creditLimit;

    public CustomersEntity() {
    }

    public CustomersEntity(int id) {
        this.id = new IntegerKey();
        this.id.setKey(id);
        contactFirstName = String.format("First [%d]", id);
        contactLastName = UUID.randomUUID().toString();
        customerName = String.format("%s, %s", contactLastName, contactFirstName);
        Random rnd = new Random(System.nanoTime());
        phone = String.format("+%d", rnd.nextInt(1000000000, Integer.MAX_VALUE));
        addressLine1 = String.format("Address 1: %s", UUID.randomUUID().toString());
        addressLine2 = String.format("Address 2: %s", UUID.randomUUID().toString());
        city = "Bangalore";
        state = "KA";
        country = "India";
        zipCode = String.valueOf(rnd.nextInt(999999));
        salesRepEmployeeNumber = rnd.nextInt();
        creditLimit = BigDecimal.valueOf(rnd.nextDouble());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomersEntity that = (CustomersEntity) o;
        return this.id.compareTo(((CustomersEntity) o).id) == 0 &&
                Objects.equals(customerName, that.customerName) &&
                Objects.equals(contactLastName, that.contactLastName) &&
                Objects.equals(contactFirstName, that.contactFirstName) &&
                Objects.equals(phone, that.phone) &&
                Objects.equals(addressLine1, that.addressLine1) &&
                Objects.equals(addressLine2, that.addressLine2) &&
                Objects.equals(city, that.city) &&
                Objects.equals(state, that.state) &&
                Objects.equals(zipCode, that.zipCode) &&
                Objects.equals(country, that.country) &&
                Objects.equals(salesRepEmployeeNumber, that.salesRepEmployeeNumber) &&
                Objects.equals(creditLimit, that.creditLimit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id.stringKey(),
                customerName,
                contactLastName,
                contactFirstName,
                phone,
                addressLine1,
                addressLine2,
                city,
                state,
                zipCode,
                country,
                salesRepEmployeeNumber,
                creditLimit);
    }

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(IntegerKey key) {
        return id.compareTo(key);
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
    public IEntity<IntegerKey> copyChanges(IEntity<IntegerKey> source, Context context) throws CopyException {
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
    public IEntity<IntegerKey> clone(Context context) throws CopyException {
        return null;
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public IntegerKey entityKey() {
        return id;
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
