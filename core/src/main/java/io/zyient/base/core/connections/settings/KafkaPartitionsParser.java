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

package io.zyient.base.core.connections.settings;

import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigValueParser;
import io.zyient.base.common.utils.DefaultLogger;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

public class KafkaPartitionsParser implements ConfigValueParser<List<Integer>> {
    public List<Integer> parse(@NonNull String value) throws Exception {
        List<Integer> partitions = new ArrayList<>();
        if (!Strings.isNullOrEmpty(value)) {
            if (value.indexOf(';') >= 0) {
                String[] parts = value.split(";");
                for (String part : parts) {
                    if (Strings.isNullOrEmpty(part)) continue;
                    Integer p = Integer.parseInt(part.trim());
                    partitions.add(p);
                    DefaultLogger.debug(String.format("Added partition; [%d]", p));
                }
            } else {
                Integer p = Integer.parseInt(value.trim());
                partitions.add(p);
                DefaultLogger.debug(String.format("Added partition; [%d]", p));
            }
        }
        if (partitions.isEmpty()) partitions.add(0);
        return partitions;
    }

    public String serialize(@NonNull List<Integer> source) throws Exception {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (int v : source) {
            if (first) first = false;
            else {
                builder.append(";");
            }
            builder.append(v);
        }
        return builder.toString();
    }
}
