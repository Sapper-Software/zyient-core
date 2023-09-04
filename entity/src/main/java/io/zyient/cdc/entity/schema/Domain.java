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

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class Domain {
    private String name;
    private EntityOptions defaultOptions;
    private long updatedTime;

    public Domain() {
        defaultOptions = new EntityOptions();
    }

    public Domain(@NonNull String name) {
        this.name = name;
        defaultOptions = new EntityOptions();
    }

    public Domain(@NonNull String name, @NonNull EntityOptions options) {
        this.name = name;
        this.defaultOptions = options;
    }

    public Domain(@NonNull String name, @NonNull Map<String, Object> options) {
        this.name = name;
        this.defaultOptions = new EntityOptions(options);
    }
}
