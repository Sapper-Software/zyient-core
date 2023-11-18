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

import com.google.common.base.Strings;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

public class IntegerListParser implements ConfigValueParser<List<Integer>> {
    @Override
    public List<Integer> parse(@NonNull String value) throws Exception {
        if (!Strings.isNullOrEmpty(value)) {
            String[] parts = value.split(",");
            List<Integer> array = new ArrayList<>(parts.length);
            for (String part : parts) {
                if (Strings.isNullOrEmpty(part)) {
                    continue;
                }
                array.add(Integer.parseInt(part));
            }
            return array;
        }
        return null;
    }

    @Override
    public String serialize(@NonNull List<Integer> value) throws Exception {
        StringBuilder builder = new StringBuilder();
        for (int v : value) {
            if (!builder.isEmpty()) {
                builder.append(",");
            }
            builder.append(v);
        }
        return builder.toString();
    }
}
