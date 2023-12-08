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

import io.zyient.base.core.content.SourceTypes;
import io.zyient.core.mapping.readers.ReadCompleteCallback;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.File;
import java.net.URI;

@Getter
@Setter
@Accessors(fluent = true)
public class InputContentInfo extends ContentInfo {
    public static final String KEY_SOURCE_TYPE = "source.type";
    public static final String KEY_SOURCE_DOC_ID = "source.document.id";
    public static final String KEY_SOURCE_URI = "source.URI";
    public static final String KEY_SOURCE_LOCAL_PATH = "source.path";
    public static final String KEY_FILE_TYPE = "reader.content.type";
    public static final String KEY_READER_NAME = "reader.name";

    private ReadCompleteCallback callback;

    public InputContentInfo() {
    }

    public InputContentInfo(@NonNull ContentInfo source) {
        super(source);
        if (source instanceof InputContentInfo) {
            if (source.containsKey(KEY_SOURCE_TYPE)) {
                sourceType(((InputContentInfo) source).sourceType());
            }
            if (source.containsKey(KEY_SOURCE_DOC_ID)) {
                documentId(((InputContentInfo) source).documentId());
            }
            if (source.containsKey(KEY_SOURCE_URI)) {
                sourceURI(((InputContentInfo) source).sourceURI());
            }
            if (source.containsKey(KEY_SOURCE_LOCAL_PATH)) {
                path(((InputContentInfo) source).path());
            }
            if (source.containsKey(KEY_FILE_TYPE)) {
                contentType(((InputContentInfo) source).contentType());
            }
            if (source.containsKey(KEY_READER_NAME)) {
                reader(((InputContentInfo) source).reader());
            }
        }
    }

    public InputContentInfo sourceURI(@NonNull URI source) {
        put(KEY_SOURCE_URI, source);
        return this;
    }

    public URI sourceURI() {
        return (URI) get(KEY_SOURCE_URI);
    }

    public InputContentInfo path(@NonNull File path) {
        put(KEY_SOURCE_LOCAL_PATH, path);
        return this;
    }

    public File path() {
        return (File) get(KEY_SOURCE_LOCAL_PATH);
    }


    public InputContentInfo contentType(@NonNull SourceTypes type) {
        put(KEY_FILE_TYPE, type);
        return this;
    }

    public SourceTypes contentType() {
        return (SourceTypes) get(KEY_FILE_TYPE);
    }

    public InputContentInfo reader(@NonNull String name) {
        put(KEY_READER_NAME, name);
        return this;
    }

    public String reader() {
        return (String) get(KEY_READER_NAME);
    }

    public InputContentInfo sourceType(@NonNull String sourceType) {
        put(KEY_SOURCE_TYPE, sourceType);
        return this;
    }

    public String sourceType() {
        return (String) get(KEY_SOURCE_TYPE);
    }

    public InputContentInfo documentId(@NonNull String documentId) {
        put(KEY_SOURCE_DOC_ID, documentId);
        return this;
    }

    public String documentId() {
        return (String) get(KEY_SOURCE_DOC_ID);
    }
}
