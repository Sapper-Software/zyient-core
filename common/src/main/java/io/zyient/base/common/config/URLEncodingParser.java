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

package io.zyient.base.common.config;

import io.zyient.base.common.GlobalConstants;
import lombok.NonNull;

import java.net.URLDecoder;
import java.net.URLEncoder;

public class URLEncodingParser implements ConfigValueParser<String>{
    @Override
    public String parse(@NonNull String value) throws Exception {
        return URLDecoder.decode(value, GlobalConstants.defaultCharset());
    }

    @Override
    public String serialize(@NonNull String value) throws Exception {
        return URLEncoder.encode(value, GlobalConstants.defaultCharset());
    }
}
