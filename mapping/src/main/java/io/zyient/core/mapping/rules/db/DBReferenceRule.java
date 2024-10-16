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

package io.zyient.core.mapping.rules.db;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.beans.BeanUtils;
import io.zyient.base.core.errors.Errors;
import io.zyient.core.mapping.rules.MappingReflectionHelper;
import io.zyient.core.mapping.rules.RuleEvaluationError;
import io.zyient.core.mapping.rules.RuleValidationError;
import io.zyient.core.persistence.Cursor;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.List;

@Getter
@Accessors(fluent = true)
public class DBReferenceRule<T, K extends IKey, E extends IEntity<K>> extends DBRule<T, K, E> {

    protected Object process(@NonNull T response,
                             @NonNull Cursor<K, E> cursor) throws RuleValidationError, RuleEvaluationError {
        try {
            List<E> entities = cursor.nextPage();
            if (entities != null && !entities.isEmpty()) {
                E entity = entities.get(0);
                if (targetMappings != null && !targetMappings.isEmpty()) {
                    for (String key : targetMappings.keySet()) {
                        FieldProperty target = targetMappings.get(key);
                        FieldProperty source = sourceFields.get(key);
                        Preconditions.checkNotNull(source);
                        Object value = BeanUtils.getValue(entity, source.field());
                        if (value != null) {
                            MappingReflectionHelper.setProperty(target.field(), target.property(), response, value);
                        }
                    }
                }
                return entity;
            }
            return null;
        } catch (Throwable t) {
            throw new RuleEvaluationError(name(),
                    entityType(),
                    expression(),
                    errorCode(),
                    Errors.getDefault().get(__ERROR_TYPE_RULES, errorCode()).getMessage(),
                    t);
        }
    }

}
