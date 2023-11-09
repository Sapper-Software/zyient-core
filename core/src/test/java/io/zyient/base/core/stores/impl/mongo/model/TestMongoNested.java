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

package io.zyient.base.core.stores.impl.mongo.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Id;
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
@Entity("test-nested")
@ToString
public class TestMongoNested {
    @Id
    private long _id;
    private String name;
    private double value;

    public TestMongoNested() {

    }

    public TestMongoNested(@NonNull TestMongoEntity entity) {
        Random rnd = new Random(System.nanoTime());
        _id = rnd.nextLong();
        name = String.format("%s-%s", entity.getKey().stringKey(), UUID.randomUUID().toString());
        value = rnd.nextDouble();
    }
}
