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
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@ToString
public class S3EventName {
    private S3EventType type;
    private String operation;

    public static S3EventName from(@NonNull String value) throws Exception {
        S3EventName name = new S3EventName();
        String[] parts = value.split(":");
        if (parts.length != 2) {
            throw new Exception(String.format("Invalid event name. [value=%s]", value));
        }
        name.type = S3EventType.parse(parts[0]);
        name.operation = parts[1];
        return name;
    }
}
