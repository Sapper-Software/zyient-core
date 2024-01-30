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

package io.zyient.base.core.auditing;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.Context;
import lombok.NonNull;

public class AuditContext extends Context {
    public static final String KEY_AUDITED = "audited";
    public static final String KEY_AUDITED_TYPE = "audited.type";

    public AuditContext() {
    }

    public AuditContext(@NonNull Context context) {
        super(context);
    }

    public AuditContext audited(@NonNull Class<?> type) {
        Preconditions.checkArgument(type.isAnnotationPresent(Audited.class));
        Audited audited = type.getAnnotation(Audited.class);
        put(KEY_AUDITED, audited);
        return (AuditContext) put(KEY_AUDITED_TYPE, type);
    }

    public Audited audited() {
        return (Audited) get(KEY_AUDITED);
    }

    public Class<?> type() {
        return (Class<?>) get(KEY_AUDITED_TYPE);
    }

    public static AuditContext of(@NonNull Class<?> type, Context context) {
        if (type.isAnnotationPresent(Audited.class)) {
            Audited audited = type.getAnnotation(Audited.class);
            if (context == null)
                return new AuditContext()
                        .audited(type);
            else
                return new AuditContext(context)
                        .audited(type);
        }
        return null;
    }
}
