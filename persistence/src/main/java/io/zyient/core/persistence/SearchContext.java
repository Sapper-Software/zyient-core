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

package io.zyient.core.persistence;

import io.zyient.base.common.model.Context;
import lombok.NonNull;

public class SearchContext extends Context {
    public static final String KEY_CURSOR_ID = "search.cursor.id";
    public static final String KEY_FETCH_CHILDREN = "search.children.fetch";

    public SearchContext cursor(@NonNull String cursorId) {
        return (SearchContext) put(KEY_CURSOR_ID, cursorId);
    }

    public String cursor() {
        return (String) get(KEY_CURSOR_ID);
    }

    public SearchContext fetchChildDocuments(boolean value) {
        return (SearchContext) put(KEY_FETCH_CHILDREN, value);
    }

    public boolean fetchChildDocuments() {
        if (containsKey(KEY_FETCH_CHILDREN))
            return (boolean) get(KEY_FETCH_CHILDREN);
        return true;
    }
}
