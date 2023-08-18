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

package ai.sapper.cdc.core.auditing;

import ai.sapper.cdc.common.model.Context;
import ai.sapper.cdc.core.model.IEntity;
import ai.sapper.cdc.core.stores.DataStoreException;

import javax.annotation.Nonnull;
import java.security.Principal;

/**
 * Interface to be implemented to setup audit context data.
 */
@SuppressWarnings("rawtypes")
public interface IAuditContextGenerator {
    /**
     * Generate the Audit Context.
     *
     * @param source  - Data Store being updated.
     * @param entity  - Entity being operated on.
     * @param context - Update context.
     * @param user    - Calling User
     * @param <E>     - Entity Type.
     * @return - Generated Audit context.
     * @throws DataStoreException
     */
    <E extends IEntity<?>> AbstractAuditContext generate(@Nonnull Object source,
                                                         @Nonnull E entity,
                                                         Context context,
                                                         Principal user) throws DataStoreException;
}
