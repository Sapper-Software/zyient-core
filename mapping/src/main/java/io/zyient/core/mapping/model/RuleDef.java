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

package io.zyient.core.mapping.model;

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.PropertyBag;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.mapping.rules.Rule;
import io.zyient.core.mapping.rules.RuleType;
import io.zyient.core.persistence.impl.rdbms.converters.PropertiesConverter;
import jakarta.persistence.Column;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Entity
@Table(name = "mp_rule_defs")
public class RuleDef implements IEntity<RuleId>, PropertyBag {
    @EmbeddedId
    private RuleId id;
    @Column(name = "rule_class")
    private String clazz;
    @Enumerated(EnumType.STRING)
    @Column(name = "rule_type")
    private RuleType type = RuleType.Transformation;
    @Column(name = "error_code")
    private Integer errorCode;
    @Column(name = "validation_code")
    private Integer validationCode;
    @Column(name = "properties")
    @Convert(converter = PropertiesConverter.class)
    private Map<String, Object> properties;

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(RuleId key) {
        return id.compareTo(key);
    }

    /**
     * Copy the changes from the specified source entity
     * to this instance.
     * <p>
     * All properties other than the Key will be copied.
     * Copy Type:
     * Primitive - Copy
     * String - Copy
     * Enum - Copy
     * Nested Entity - Copy Recursive
     * Other Objects - Copy Reference.
     *
     * @param source  - Source instance to Copy from.
     * @param context - Execution context.
     * @return - Copied Entity instance.
     * @throws CopyException
     */
    @Override
    public IEntity<RuleId> copyChanges(IEntity<RuleId> source, Context context) throws CopyException {
        throw new CopyException("Method not implemented...");
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<RuleId> clone(Context context) throws CopyException {
        throw new CopyException("Method not implemented...");
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public RuleId entityKey() {
        return id;
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {
        ValidationExceptions errors = null;
        errors = ValidationExceptions.checkCondition(id != null, "Rule ID not set...", errors);
        errors = ValidationExceptions.checkValue(id.getNamespace(), "Rule namespace not set...", errors);
        errors = ValidationExceptions.checkValue(id.getName(), "Rule name not set...", errors);
        errors = ValidationExceptions.checkValue(clazz, "Rule Class not specified...", errors);
        errors = ValidationExceptions.checkCondition(type != null, "Rule type not specified...", errors);
        if (errors != null) {
            throw errors;
        }
    }

    @Override
    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @Override
    public boolean hasProperty(@NonNull String name) {
        if (properties != null) {
            return properties.containsKey(name);
        }
        return false;
    }

    @Override
    public Object getProperty(@NonNull String name) {
        if (properties != null) {
            return properties.get(name);
        }
        return null;
    }

    @Override
    public PropertyBag setProperty(@NonNull String name,
                                   @NonNull Object value) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(name, value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Rule<?> from(@NonNull BaseEnv<?> env) throws Exception {
        Class<? extends Rule<?>> clz = (Class<? extends Rule<?>>) Class.forName(clazz);

        return clz.getDeclaredConstructor()
                .newInstance()
                .configure(this, env);
    }
}
