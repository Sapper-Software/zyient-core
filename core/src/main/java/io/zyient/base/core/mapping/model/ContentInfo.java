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

package io.zyient.base.core.mapping.model;

import io.zyient.base.common.model.Context;
import io.zyient.base.core.model.UserContext;
import lombok.NonNull;

import java.security.Principal;

public abstract class ContentInfo extends Context implements UserContext {
    public static final String KEY_MAPPER_NAME = "mapping.name";
    public static final String KEY_USER = "user.principle";
    public static final String KEY_REFERENCE_ID = "reference.id";

    public ContentInfo mapping(@NonNull String name) {
        put(KEY_MAPPER_NAME, name);
        return this;
    }

    public String mapping() {
        return (String) get(KEY_MAPPER_NAME);
    }

    @Override
    public UserContext user(@NonNull Principal user) {
        put(KEY_USER, user);
        return this;
    }

    @Override
    public Principal user() {
        return (Principal) get(KEY_USER);
    }

    public ContentInfo referenceId(@NonNull String referenceId) {
        put(KEY_REFERENCE_ID, referenceId);
        return this;
    }

    public String referenceId() {
        return (String) get(KEY_REFERENCE_ID);
    }
}
