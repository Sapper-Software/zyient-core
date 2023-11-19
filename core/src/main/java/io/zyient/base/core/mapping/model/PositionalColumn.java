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

package io.zyient.base.core.mapping.model;

import io.zyient.base.common.config.Config;
import io.zyient.base.common.model.ValidationException;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PositionalColumn extends Column {
    @Config(name = "start", required = false, type = Integer.class)
    private int start = -1;
    @Config(name = "end", required = false, type = Integer.class)
    private int end = -1;
    @Config(name = "length", required = false, type = Integer.class)
    private int length = -1;

    @Override
    public void validate() throws ValidationException {
        super.validate();
        if (end <= 0 && length <= 0) {
            throw new ValidationException("One of [end] or [length] must be specified...");
        }
        if (end > 0 && length > 0) {
            throw new ValidationException("Only one of [end] or [length] must be specified...");
        }
    }
}
