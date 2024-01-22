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

package io.zyient.core.extraction.model;

import io.zyient.base.common.model.Context;
import lombok.NonNull;

public class DocumentMetadata extends Context {
    public static final String KEY_LANGUAGE = "document.language";
    public static final String KEY_PAGE_COUNT = "pages.count";

    public DocumentMetadata language(@NonNull String languageCode) {
        return (DocumentMetadata) put(KEY_LANGUAGE, languageCode);
    }

    public String language() {
        return (String) get(KEY_LANGUAGE);
    }

    public DocumentMetadata pageCount(int count) {
        return (DocumentMetadata) put(KEY_PAGE_COUNT, count);
    }

    public Integer pageCount() {
        return (Integer) get(KEY_PAGE_COUNT);
    }
}
