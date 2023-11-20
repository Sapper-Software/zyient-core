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

package io.zyient.base.common.config;

import io.zyient.base.common.utils.ReflectionUtils;
import lombok.NonNull;

import java.util.Map;

public class StringMapParser implements ConfigValueParser<Map<String, String>> {
    @Override
    public Map<String, String> parse(@NonNull String value) throws Exception {
        return ReflectionUtils.mapFromString(value);
    }

    @Override
    public String serialize(@NonNull Map<String, String> value) throws Exception {
        if (!value.isEmpty()) {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<String, String> entry : value.entrySet()) {
                if (builder.isEmpty()) {
                    builder.append(";");
                }
                builder.append(entry.getKey())
                        .append("=")
                        .append(entry.getValue());
            }
            return builder.toString();
        }
        return null;
    }
}
