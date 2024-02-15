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

package io.zyient.core.mapping.rules;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.Settings;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.common.utils.beans.BeanUtils;
import io.zyient.core.mapping.model.RuleDef;
import io.zyient.core.mapping.model.RuleId;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.lang.reflect.Field;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class RuleConfig extends Settings {
    public static final String CONFIG_NAMESPACE = "namespace";
    public static final String CONFIG_NAME = "name";
    public static final String CONFIG_TYPE = "ruleType";
    public static final String CONFIG_ERROR_CODE = "errorCode";
    public static final String CONFIG_VAL_ERROR_CODE = "validationErrorCode";

    @Config(name = CONFIG_NAMESPACE, required = false)
    private String namespace = "default";
    @Config(name = CONFIG_NAME)
    private String name;
    @Config(name = CONFIG_TYPE, required = false, type = RuleType.class)
    private RuleType type = RuleType.Transformation;
    @Config(name = CONFIG_ERROR_CODE, required = false, type = Integer.class)
    private Integer errorCode;
    @Config(name = CONFIG_VAL_ERROR_CODE, required = false, type = Integer.class)
    private Integer validationErrorCode;

    public RuleConfig from(@NonNull RuleDef def) throws ConfigurationException {
        namespace = def.getId().getNamespace();
        name = def.getId().getName();
        type = def.getType();
        errorCode = def.getErrorCode();
        validationErrorCode = def.getValidationCode();
        if (def.getProperties() != null) {
            try {
                ConfigReader.from(this, def.getProperties(), false);
            } catch (Exception e) {
                throw new ConfigurationException(e);
            }
        }
        validate();
        return this;
    }

    public RuleDef to() throws ConfigurationException {
        validate();
        RuleDef def = new RuleDef();
        def.setId(new RuleId(namespace, name));
        def.setType(type);
        def.setClazz(getClass().getCanonicalName());
        def.setErrorCode(errorCode);
        def.setValidationCode(validationErrorCode);
        try {
            Field[] fields = ReflectionHelper.getAllFields(getClass());
            if (fields != null) {
                for (Field field : fields) {
                    if (!field.isAnnotationPresent(Config.class)) continue;
                    Config cfg = field.getAnnotation(Config.class);
                    if (cfg.name().compareTo(CONFIG_NAMESPACE) == 0
                            || cfg.name().compareTo(CONFIG_NAME) == 0
                            || cfg.name().compareTo(CONFIG_TYPE) == 0
                            || cfg.name().compareTo(CONFIG_ERROR_CODE) == 0
                            || cfg.name().compareTo(CONFIG_VAL_ERROR_CODE) == 0) continue;
                    Object value = BeanUtils.getValue(this, field.getName());
                    if (value != null)
                        def.setProperty(cfg.name(), value);
                }
            }
            return def;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public void validate() throws ConfigurationException {
        if (getType() == RuleType.Validation) {
            if (validationErrorCode == null) {
                throw new ConfigurationException(
                        String.format("Missing required property [validationErrorCode]. [rule=%s]", getName()));
            }
        }
    }

    public abstract <E> Rule<E> createInstance(@NonNull Class<? extends E> type) throws Exception;
}
