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

import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.utils.SourceTypes;
import lombok.NonNull;

import java.io.File;

public class OutputContentInfo extends ContentInfo {
    public static final String KEY_OUTPUT_TYPE = "output.type";
    public static final String KEY_OUTPUT_LOCAL_PATH = "output.path";
    public static final String KEY_FILE_TYPE = "writer.content.type";
    public static final String KEY_WRITER_NAME = "writer.name";
    public static final String KEY_QUERY = "query";

    public OutputContentInfo query(@NonNull AbstractDataStore.Q query) {
        put(KEY_QUERY, query);
        return this;
    }

    public AbstractDataStore.Q query() {
        return (AbstractDataStore.Q) get(KEY_QUERY);
    }

    public File path() {
        return (File) get(KEY_OUTPUT_LOCAL_PATH);
    }


    public OutputContentInfo contentType(@NonNull SourceTypes type) {
        put(KEY_FILE_TYPE, type);
        return this;
    }

    public SourceTypes contentType() {
        return (SourceTypes) get(KEY_FILE_TYPE);
    }

    public OutputContentInfo writer(@NonNull String name) {
        put(KEY_WRITER_NAME, name);
        return this;
    }

    public String writer() {
        return (String) get(KEY_WRITER_NAME);
    }

    public OutputContentInfo outputType(@NonNull String outputType) {
        put(KEY_OUTPUT_TYPE, outputType);
        return this;
    }

    public String outputType() {
        return (String) get(KEY_OUTPUT_TYPE);
    }
}
