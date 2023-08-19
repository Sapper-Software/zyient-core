/*
 *  Copyright (2020) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package ai.sapper.cdc.core.stores;

import ai.sapper.cdc.common.utils.ReflectionUtils;
import ai.sapper.cdc.core.model.IEntity;
import ai.sapper.cdc.core.stores.annotations.EGeneratedType;
import ai.sapper.cdc.core.stores.annotations.GeneratedId;
import com.google.common.base.Preconditions;
import lombok.NonNull;

import javax.persistence.EmbeddedId;
import javax.persistence.Id;
import java.lang.reflect.Field;
import java.util.UUID;

public class IDGenerator {
    public static void process(@NonNull IEntity<?> entity,
                               @NonNull AbstractDataStore<?> dataStore) throws DataStoreException {
        Field[] fields = ReflectionUtils.getAllFields(entity.getClass());
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
                        ReflectionUtils.setObjectValue(entity, field, UUID.randomUUID().toString());
                    } else {
                        Long value = dataStore.nextSequence(gi.sequence());
                        ReflectionUtils.setObjectValue(entity, field, value);
                    }
                }
            } else {
                Class<?> type = field.getType();
                Field[] fields = ReflectionUtils.getAllFields(type);
                if (fields != null) {
                    for (Field f : fields) {
                        if (f.isAnnotationPresent(GeneratedId.class)) {
                            Object fv = ReflectionUtils.getFieldValue(entity, field);
                            if (fv == null) {
                                fv = ReflectionUtils.createInstance(field.getType());
                                ReflectionUtils.setObjectValue(entity, field, fv);
                            }
                            GeneratedId gi = f.getAnnotation(GeneratedId.class);
                            if (gi.type() == EGeneratedType.UUID) {
                                ReflectionUtils.setObjectValue(fv, f, UUID.randomUUID().toString());
                            } else {
                                Long value = dataStore.nextSequence(gi.sequence());
                                ReflectionUtils.setObjectValue(fv, f, value);
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
