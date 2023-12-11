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

package io.zyient.core.sdk.request;

import io.zyient.base.common.model.ValidationException;
import io.zyient.base.core.model.UserOrRole;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
public class ContentCreateRequest {
    private String id;
    private String name;
    private String sourcePath;
    private String mimeType;
    private UserOrRole creator;

    public ContentCreateRequest() {
        id = UUID.randomUUID().toString();
    }

    public void validate() throws ValidationException {
        ValidationException.check(this, "name");
        ValidationException.check(this, "sourcePath");
        ValidationException.check(this, "creator");
    }
}
