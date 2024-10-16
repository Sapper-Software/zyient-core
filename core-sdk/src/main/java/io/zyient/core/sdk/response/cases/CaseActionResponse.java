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

package io.zyient.core.sdk.response.cases;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.core.sdk.model.caseflow.CaseEntity;
import io.zyient.core.sdk.request.Request;
import io.zyient.core.sdk.response.Response;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class CaseActionResponse<R extends Request, E extends Enum<E>> extends Response<R> {
    private CaseEntity<E> caseEntity;

    public CaseActionResponse() {
    }

    public CaseActionResponse(@NonNull Exception error) {
        setError(error);
    }
}
