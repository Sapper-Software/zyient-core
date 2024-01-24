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

package io.zyient.core.persistence;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.core.persistence.annotations.EGeneratedType;
import io.zyient.core.persistence.annotations.GeneratedId;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Id;
import lombok.NonNull;

import java.lang.reflect.Field;
import java.util.UUID;

public class IDGenerator {
    public static void process(@NonNull IEntity<?> entity,
                               @NonNull AbstractDataStore<?> dataStore) throws DataStoreException {
        Field[] fields = ReflectionHelper.getAllFields(entity.getClass());
        if (fields != null) {
            for (Field field : fields) {
                if (field.isAnnotationPresent(Id.class) || field.isAnnotationPresent(EmbeddedId.class)) {
                    process(entity, field, dataStore);
                    break;
                }
            }
        }
    }

    public static void process(@NonNull IEntity<?> entity,
                               @NonNull Field field,
                               @NonNull AbstractDataStore<?> dataStore) throws DataStoreException {
        Preconditions.checkArgument(field.isAnnotationPresent(Id.class) || field.isAnnotationPresent(EmbeddedId.class));
        try {
            if (field.isAnnotationPresent(Id.class)) {
                if (field.isAnnotationPresent(GeneratedId.class)) {
                    GeneratedId gi = field.getAnnotation(GeneratedId.class);
                    if (gi.type() == EGeneratedType.UUID) {
                        ReflectionHelper.setStringValue(entity, field, UUID.randomUUID().toString());
                    } else {
                        Long value = dataStore.nextSequence(gi.sequence());
                        ReflectionHelper.setLongValue(entity, field, value);
                    }
                }
            } else {
                Class<?> type = field.getType();
                Field[] fields = ReflectionHelper.getAllFields(type);
                if (fields != null) {
                    for (Field f : fields) {
                        if (f.isAnnotationPresent(GeneratedId.class)) {
                            Object fv = ReflectionHelper.reflectionUtils().getFieldValue(entity, field);
                            if (fv == null) {
                                fv = ReflectionHelper.createInstance(field.getType());
                                ReflectionHelper.setValue(fv, entity, field);
                            }
                            GeneratedId gi = f.getAnnotation(GeneratedId.class);
                            if (gi.type() == EGeneratedType.UUID) {
                                ReflectionHelper.setStringValue(fv, f, UUID.randomUUID().toString());
                            } else {
                                Long value = dataStore.nextSequence(gi.sequence());
                                ReflectionHelper.setLongValue(fv, f, value);
                            }
                            break;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }
}
