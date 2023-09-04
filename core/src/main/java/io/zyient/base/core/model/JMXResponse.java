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

package io.zyient.base.core.model;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
public class JMXResponse {
    public static final String KEY_BEAN_NAME = "name";

    private List<Map<String, String>> beans;

    public Map<String, String> findBeanByName(@NonNull String regex) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(regex));
        if (beans != null && !beans.isEmpty()) {
            Pattern pattern = Pattern.compile(regex);
            for (Map<String, String> bean : beans) {
                if (bean.containsKey(KEY_BEAN_NAME)) {
                    String name = bean.get(KEY_BEAN_NAME);
                    Matcher m = pattern.matcher(name);
                    if (m.matches()) return bean;
                }
            }
        }
        return null;
    }
}
