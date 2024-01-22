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

package io.zyient.core.filesystem.sync.s3.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class S3EventObject {
    public static final String KEY_KEY = "key";
    public static final String KEY_SIZE = "size";
    public static final String KEY_TAG = "eTag";
    public static final String KEY_VERSION = "versionId";

    private String key;
    private int size;
    private String tag;
    private String versionId;

    public static S3EventObject from(@NonNull Map<String, Object> data) throws Exception {
        S3EventObject object = new S3EventObject();
        object.key = (String) data.get(KEY_KEY);
        if (Strings.isNullOrEmpty(object.key)) {
            throw new Exception("Object key not defined...");
        }
        Object s = data.get(KEY_SIZE);
        if (s instanceof Integer)
            object.size = (int) s;
        else
            object.size = 0;
        object.tag = (String) data.get(KEY_TAG);
        object.versionId = (String) data.get(KEY_VERSION);
        return object;
    }
}
