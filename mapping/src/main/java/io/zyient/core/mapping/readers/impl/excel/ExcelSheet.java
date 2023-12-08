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

package io.zyient.core.mapping.readers.impl.excel;

import com.google.common.base.Strings;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.model.ValidationException;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ExcelSheet {
    @Config(name = "index", required = false, type = Integer.class)
    private Integer index;
    @Config(name = "name", required = false)
    private String name;

    public void validate() throws ValidationException {
        if ((index == null || index < 0) && Strings.isNullOrEmpty(name)) {
            throw new ValidationException("Either sheet index or name must be specified...");
        }
    }
}
