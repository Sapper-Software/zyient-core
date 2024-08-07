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

package io.zyient.cdc.entity.types;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class TextType extends SizedDataType<String> {
    private static final long DEFAULT_SIZE = 1024 * 64;

    public TextType() {

    }

    public TextType(@NonNull String name, int jdbcType, long size) {
        super(name, String.class, jdbcType, size);
        if (getSize() <= 0) setSize(DEFAULT_SIZE);
    }

    public TextType(@NonNull TextType source, long size) {
        super(source, size);
    }

    public String checkSize(@NonNull String data) {
        if (data.length() > getSize()) {
            data = data.substring(0, (int) getSize());
        }
        return data;
    }
}
