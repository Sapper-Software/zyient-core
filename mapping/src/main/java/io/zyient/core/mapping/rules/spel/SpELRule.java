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

package io.zyient.core.mapping.rules.spel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.beans.PropertyDef;
import io.zyient.core.mapping.rules.MappingReflectionHelper;
import io.zyient.core.mapping.rules.RuleConfig;
import io.zyient.core.mapping.rules.RuleType;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.util.ArrayList;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class SpELRule<T> extends AbstractSpELRule<T> {

    @Override
    protected List<FieldMap> createTargetFields(RuleConfig config) throws Exception {
        Preconditions.checkArgument(config instanceof SpELRuleConfig);
        SpELRuleConfig spELRuleConfig = ((SpELRuleConfig) config);
        List<FieldMap> fieldMaps = new ArrayList<>();
        if (spELRuleConfig.getTarget() != null) {
            String target = MappingReflectionHelper.normalizeField(spELRuleConfig.getTarget());
            PropertyDef property = MappingReflectionHelper.findField(spELRuleConfig.getTarget(), entityType());
            if (property == null) {
                throw new ConfigurationException(String.format("Failed to find property. [type=%s][property=%s]",
                        entityType().getCanonicalName(), spELRuleConfig.getTarget()));
            }
            fieldMaps.add(new FieldMap(target, null, property));
        }

        return fieldMaps;
    }

    @Override
    protected void validate(RuleConfig config) throws ConfigurationException {
        Preconditions.checkArgument(config instanceof SpELRuleConfig);
        SpELRuleConfig spELRuleConfig = ((SpELRuleConfig) config);
        if (getRuleType() == RuleType.Transformation) {
            if (Strings.isNullOrEmpty(spELRuleConfig.getTarget())) {
                throw new ConfigurationException(String.format("[rule=%s] [Target] not specified.", name()));
            }
        }
    }
}
