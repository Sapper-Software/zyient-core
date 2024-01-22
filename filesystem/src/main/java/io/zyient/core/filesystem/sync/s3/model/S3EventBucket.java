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
import io.zyient.base.common.utils.JSONUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class S3EventBucket {
    public static final String KEY_NAME = "name";
    public static final String KEY_OWNER = "ownerIdentity.principalId";
    public static final String KEY_ARN = "arn";

    private String name;
    private String ownerPrincipalId;
    private String ARN;

    public static S3EventBucket from(@NonNull Map<String, Object> data) throws Exception {
        S3EventBucket bucket = new S3EventBucket();
        bucket.name = (String) data.get(KEY_NAME);
        if (Strings.isNullOrEmpty(bucket.name)) {
            throw new Exception("Bucket name not found...");
        }
        bucket.ownerPrincipalId = (String) JSONUtils.find(data, KEY_OWNER);
        if (Strings.isNullOrEmpty(bucket.ownerPrincipalId)) {
            throw new Exception("Bucket owner not found...");
        }
        bucket.ARN = (String) data.get(KEY_ARN);
        if (Strings.isNullOrEmpty(bucket.ARN)) {
            throw new Exception("Bucket ARN not found...");
        }
        return bucket;
    }
}
