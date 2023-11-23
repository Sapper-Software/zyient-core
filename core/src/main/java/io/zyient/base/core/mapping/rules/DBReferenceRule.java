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

import io.zyient.base.core.mapping.model.MappedResponse;
import io.zyient.base.core.stores.impl.rdbms.HibernateConnection;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

@Getter
@Setter
@Accessors(fluent = true)
public class DBReferenceRule<T> extends ExternalRule<T> {
    private HibernateConnection connection;

    @Override
    protected Object doEvaluate(@NonNull MappedResponse<T> data) throws Exception {
        return null;
    }

    @Override
    protected void setup(@NonNull RuleConfig config) throws ConfigurationException {

    }
}
