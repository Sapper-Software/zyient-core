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

package io.zyient.core.sdk.request.content;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.model.ValidationException;
import io.zyient.core.sdk.model.content.ContentId;
import io.zyient.core.sdk.request.Request;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ContentDeleteRequest extends Request {
    private ContentId contentId;

    public void validate() throws ValidationException {
        super.validate();
        ValidationException.check(this, "contentId");
        ValidationException.check(contentId, "collection");
        ValidationException.check(contentId, "id");
    }
}
