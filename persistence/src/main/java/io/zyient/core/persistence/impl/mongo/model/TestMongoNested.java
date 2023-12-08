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

package io.zyient.core.persistence.impl.mongo.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dev.morphia.annotations.*;
import dev.morphia.utils.IndexType;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.StringKey;
import io.zyient.core.persistence.impl.mongo.MongoEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.util.Random;
import java.util.UUID;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@Entity("TestNested")
@ToString
@Indexes({
        @Index(fields = @Field(value = "parent", type = IndexType.DESC))
})
public class TestMongoNested extends MongoEntity<StringKey> {
    private StringKey key;
    @Property("parent")
    private String parentId;
    private String name;
    private double value;

    public TestMongoNested() {
        key = new StringKey(UUID.randomUUID().toString());
        parentId = UUID.randomUUID().toString();
        name = "Test Nested entity";
        value = System.nanoTime();
        getState().setState(EEntityState.New);
    }

    /**
     * Validate this entity instance.
     *
     * @param errors
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public ValidationExceptions doValidate(ValidationExceptions errors) throws ValidationExceptions {
        return errors;
    }

    public TestMongoNested(@NonNull TestMongoEntity entity) {
        key = new StringKey(UUID.randomUUID().toString());
        Random rnd = new Random(System.nanoTime());
        parentId = entity.entityKey().stringKey();
        name = String.format("%s-%s", entity.getKey().stringKey(), UUID.randomUUID().toString());
        value = rnd.nextDouble();
        getState().setState(EEntityState.New);
    }

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(StringKey key) {
        return this.key.compareTo(key);
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<StringKey> clone(Context context) throws CopyException {
        return null;
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public StringKey entityKey() {
        return key;
    }
}
