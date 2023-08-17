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

import ai.sapper.cdc.core.model.IEntity;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;

@Getter
@Setter
public class BaseSearchResult<T extends IEntity<?>> {
    private Class<? extends IEntity<?>> type;
    private String query;
    private int count;
    private long totalRecords;
    private int offset;
    private String scrollId;

    public BaseSearchResult(@Nonnull Class<? extends IEntity<?>> type) {
        this.type = type;
    }
}
