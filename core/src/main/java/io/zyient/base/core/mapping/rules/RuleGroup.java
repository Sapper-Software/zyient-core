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

package io.zyient.base.core.mapping.rules;

import com.google.common.base.Preconditions;
import io.zyient.base.core.mapping.model.MappedResponse;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

@Getter
@Accessors(fluent = true)
public class RuleGroup<T> extends BaseRule<T> {

    @Override
    protected Object doEvaluate(@NonNull MappedResponse<T> data) throws RuleValidationError, RuleEvaluationError {
        return true;
    }

    @Override
    protected void setup(@NonNull RuleConfig config) throws ConfigurationException {
        Preconditions.checkArgument(config instanceof RuleGroupConfig);
    }
}
