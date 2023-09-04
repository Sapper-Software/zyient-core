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

package io.zyient.base.core.stores;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.auditing.AbstractAuditContext;
import io.zyient.base.core.auditing.IAuditContextGenerator;

import javax.annotation.Nonnull;
import java.security.Principal;

public class DataSourceContextGenerator implements IAuditContextGenerator {
    /**
     * Generate the Audit Context.
     *
     * @param source  - Data Store being updated.
     * @param entity  - Entity being operated on.
     * @param context - Update context.
     * @param user    - Calling User
     * @return - Generated Audit context.
     * @throws DataStoreException
     */
    @Override
    @SuppressWarnings("rawtypes")
    public <E extends IEntity<?>> AbstractAuditContext generate(@Nonnull Object source,
                                                                @Nonnull E entity,
                                                                Context context,
                                                                Principal user) throws DataStoreException {
        Preconditions.checkArgument(source instanceof AbstractDataStore);
        try {
            AbstractDataStore store = (AbstractDataStore) source;
            return store.context();
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }
}
