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

package io.zyient.core.content;

import io.zyient.base.common.model.Context;
import io.zyient.base.core.model.UserContext;
import lombok.NonNull;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

public class DocumentContext extends Context implements UserContext {
    public static final String KEY_USER_PRINCIPAL = "user.principal";
    public static final String KEY_DOC_UNPACK = "document.unpack";
    public static final String KEY_DOC_DECRYPT = "document.decrypt";
    public static final String KEY_DOC_CUSTOM_FIELDS = "document.custom.fields";

    public DocumentContext() {
        unpack(true);
        decrypt(false);
    }

    public DocumentContext(@NonNull Context context) {
        super(context);
    }

    @Override
    public UserContext user(@NonNull Principal user) {
        return (UserContext) put(KEY_USER_PRINCIPAL, user);
    }

    @Override
    public Principal user() {
        return (Principal) get(KEY_USER_PRINCIPAL);
    }

    public DocumentContext unpack(boolean value) {
        return (DocumentContext) put(KEY_DOC_UNPACK, value);
    }

    public boolean unpack() {
        return (boolean) get(KEY_DOC_UNPACK);
    }

    public DocumentContext decrypt(boolean value) {
        return (DocumentContext) put(KEY_DOC_DECRYPT, value);
    }

    public boolean decrypt() {
        return (boolean) get(KEY_DOC_DECRYPT);
    }

    @SuppressWarnings("unchecked")
    public DocumentContext addField(@NonNull String name,
                                    Object value) {
        Map<String, Object> values = null;
        if (containsKey(KEY_DOC_CUSTOM_FIELDS)) {
            values = (Map<String, Object>) get(KEY_DOC_CUSTOM_FIELDS);
        } else {
            values = new HashMap<>();
            put(KEY_DOC_CUSTOM_FIELDS, values);
        }
        values.put(name, value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> customFields() {
        return (Map<String, Object>) get(KEY_DOC_CUSTOM_FIELDS);
    }
}
