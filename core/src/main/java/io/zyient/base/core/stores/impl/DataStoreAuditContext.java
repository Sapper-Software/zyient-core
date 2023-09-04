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

package io.zyient.base.core.stores.impl;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zyient.base.common.GlobalConstants;
import io.zyient.base.core.auditing.AbstractAuditContext;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DataStoreAuditContext extends AbstractAuditContext {
    private String type;
    private String name;
    private String connectionType;
    private String connectionName;

    /**
     * Get the context as a JSON string.
     *
     * @return - JSON String
     * @throws JsonGenerationException
     */
    @Override
    public String json() throws JsonGenerationException {
        try {
            ObjectMapper mapper = GlobalConstants.getJsonMapper();
            return mapper.writeValueAsString(this);
        } catch (Exception ex) {
            throw new JsonGenerationException(ex);
        }
    }
}
