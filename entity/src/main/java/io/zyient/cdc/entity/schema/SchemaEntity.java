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

package io.zyient.cdc.entity.schema;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.cdc.entity.model.EntityOptions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;
import java.util.Objects;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class SchemaEntity {
    private String domain;
    private int group = -1;
    private String entity;
    private boolean enabled = false;
    private EntityOptions options;
    private long updatedTime;

    public SchemaEntity() {
        options = new EntityOptions();
    }

    public SchemaEntity(@NonNull String domain, @NonNull String entity) {
        this.domain = domain;
        this.entity = entity;
        options = new EntityOptions();
    }

    public SchemaEntity(@NonNull String domain,
                        @NonNull String entity,
                        @NonNull EntityOptions options) {
        this.domain = domain;
        this.entity = entity;
        this.options = options;
    }

    public SchemaEntity(@NonNull String domain,
                        @NonNull String entity,
                        @NonNull Map<String, Object> options) {
        this.domain = domain;
        this.entity = entity;
        this.options = new EntityOptions(options);
    }

    public SchemaEntity(@NonNull SchemaEntity source) {
        this.domain = source.getDomain();
        this.entity = source.getEntity();
        this.group = source.getGroup();
        this.options = new EntityOptions(source.options);
    }

    public EntityOptions withOptions(@NonNull Map<String, Object> options) {
        this.options = new EntityOptions(options);
        return this.options;
    }


    public EntityOptions addOption(@NonNull String option, @NonNull Object value) {
        if (options == null) {
            options = new EntityOptions();
        }
        options.put(option, value);
        return options;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaEntity that = (SchemaEntity) o;
        return domain.equals(that.domain) && Objects.equals(group, that.group) && entity.equals(that.entity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(domain, group, entity);
    }

    @Override
    public String toString() {
        return String.format("[domain=%s][entity=%s][group=%d]", domain, entity, group);
    }

    public static String key(@NonNull SchemaEntity entity) {
        return key(entity.getDomain(), entity.getEntity());
    }

    public static String key(@NonNull String domain, @NonNull String entity) {
        return String.format("%s::%s", domain, entity);
    }
}
