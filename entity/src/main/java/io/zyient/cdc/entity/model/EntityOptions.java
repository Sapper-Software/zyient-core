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

package io.zyient.cdc.entity.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.model.Options;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class EntityOptions extends Options {
    public static final String __CONFIG_PATH = "entityOptions";

    public static final String OPTION_AUTO_CREATE_ENTITY = "autoCreateEntity";
    public static final String OPTION_AUTO_ALTER_ENTITY = "autoAlterEntity";
    public static final String OPTION_ERROR_ON_DROPPED_COLUMN = "errorOnDroppedColumn";
    public static final String OPTION_IGNORE_DROPPED_COLUMN = "ignoreDroppedColumn";
    public static final String OPTION_IGNORE_DUPLICATE_ERROR = "ignoreDuplicates";
    public static final String OPTION_IGNORE_DELETES = "ignoreDeletes";
    public static final String OPTION_SKIP_BAD_RECORDS = "skipBadRecords";
    public static final String OPTION_IGNORE_DROP_ENTITY = "ignoreEntityDrop";

    public EntityOptions() {
        super(__CONFIG_PATH);
        setupDefaults();
    }

    public EntityOptions(@NonNull Options source) {
        super(source);
        setConfigPath(__CONFIG_PATH);
        setupDefaults();
    }

    public EntityOptions(@NonNull Map<String, Object> options) {
        super(options);
        setConfigPath(__CONFIG_PATH);
        setupDefaults();
    }

    public EntityOptions withOptions(@NonNull Map<String, String> options) {
        for (Map.Entry<String, String> param : options.entrySet()) {
            put(param.getKey(), param.getValue());
        }
        return this;
    }

    public EntityOptions merge(@NonNull EntityOptions options) {
        if (!options.isEmpty()) {
            put(OPTION_AUTO_CREATE_ENTITY, options.autoCreateEntity());
            if (!options.autoAlterEntity()) {
                put(OPTION_AUTO_ALTER_ENTITY, true);
            }
            if (options.ignoreDropEntity()) {
                put(OPTION_IGNORE_DROP_ENTITY, true);
            }
        }
        return this;
    }

    public void setupDefaults() {
        if (getOptions() == null)
            setOptions(new HashMap<>());
        if (!containsKey(OPTION_AUTO_CREATE_ENTITY)) {
            put(OPTION_AUTO_CREATE_ENTITY, true);
        }
        if (!containsKey(OPTION_AUTO_ALTER_ENTITY))
            put(OPTION_AUTO_ALTER_ENTITY, true);
        if (!containsKey(OPTION_IGNORE_DROP_ENTITY))
            put(OPTION_IGNORE_DROP_ENTITY, false);
        if (!containsKey(OPTION_ERROR_ON_DROPPED_COLUMN))
            put(OPTION_ERROR_ON_DROPPED_COLUMN, false);
        if (!containsKey(OPTION_IGNORE_DUPLICATE_ERROR))
            put(OPTION_IGNORE_DUPLICATE_ERROR, true);
        if (!containsKey(OPTION_IGNORE_DELETES))
            put(OPTION_IGNORE_DELETES, false);
        if (!containsKey(OPTION_IGNORE_DROPPED_COLUMN))
            put(OPTION_IGNORE_DROPPED_COLUMN, true);
        if (!containsKey(OPTION_SKIP_BAD_RECORDS)) {
            put(OPTION_SKIP_BAD_RECORDS, true);
        }
    }

    public boolean autoCreateEntity() {
        return getBoolean(OPTION_AUTO_CREATE_ENTITY).orElse(false);
    }

    public boolean autoAlterEntity() {
        return getBoolean(OPTION_AUTO_ALTER_ENTITY).orElse(false);
    }

    public boolean ignoreDropEntity() {
        return getBoolean(OPTION_IGNORE_DROP_ENTITY).orElse(false);
    }

    public boolean errorOnDroppedColumn() {
        return getBoolean(OPTION_ERROR_ON_DROPPED_COLUMN).orElse(false);
    }

    public boolean ignoreDroppedColumn() {
        return getBoolean(OPTION_IGNORE_DROPPED_COLUMN).orElse(false);
    }

    public boolean ignoreDuplicates() {
        return getBoolean(OPTION_IGNORE_DUPLICATE_ERROR).orElse(false);
    }

    public boolean ignoreDeletes() {
        return getBoolean(OPTION_IGNORE_DELETES).orElse(false);
    }

    public boolean skipBadRecords() {
        return getBoolean(OPTION_SKIP_BAD_RECORDS).orElse(false);
    }
}
