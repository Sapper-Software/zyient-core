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

package io.zyient.core.filesystem;

import com.google.common.base.Strings;
import io.zyient.base.common.utils.PathUtils;
import lombok.NonNull;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class FSPathUtils extends PathUtils {

    public static String encode(@NonNull String key) {
        key = key.trim();
        boolean start = key.startsWith("/");
        boolean ends = key.endsWith("/");
        String[] parts = key.split("/");
        StringBuilder builder = new StringBuilder();
        int count = 0;
        for (String part : parts) {
            if (Strings.isNullOrEmpty(part)) continue;
            if (count == 0) {
                if (start)
                    builder.append("/");
            } else {
                builder.append("/");
            }
            part = part.replaceAll("\\s", "-");
            String p = URLEncoder.encode(part, StandardCharsets.UTF_8);
            builder.append(p);
            count++;
        }
        if (ends) {
            builder.append("/");
        }
        return builder.toString();
    }

    public static String decode(@NonNull String key) {
        key = key.trim();
        boolean start = key.startsWith("/");
        boolean ends = key.endsWith("/");
        String[] parts = key.split("/");
        StringBuilder builder = new StringBuilder();
        int count = 0;
        for (String part : parts) {
            if (Strings.isNullOrEmpty(part)) continue;
            if (count == 0) {
                if (start)
                    builder.append("/");
            } else {
                builder.append("/");
            }
            String p = URLDecoder.decode(part, StandardCharsets.UTF_8);
            builder.append(p);
            count++;
        }
        if (ends) {
            builder.append("/");
        }
        return builder.toString();
    }
}
