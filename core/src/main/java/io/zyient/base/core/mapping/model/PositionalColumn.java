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
    @Config(name = "positional.start", required = false, type = Integer.class)
    private Integer posStart;
    @Config(name = "positional.end", required = false, type = Integer.class)
    private Integer posEnd;
    @Config(name = "positional.length", required = false, type = Integer.class)
    private Integer length;

    public PositionalColumn() {
    }

    public PositionalColumn(String name,
                            Integer index,
                            Integer posStart,
                            Integer posEnd,
                            Integer length) {
        super(name, index);
        this.posStart = posStart;
        this.posEnd = posEnd;
        this.length = length;
    }

    public void validate() throws ValidationException {
        if (posStart != null) {
            if (posEnd == null && length == null) {
                throw new ValidationException("End Position or Length must be specified for positional columns.");
            }
        }
    }
}
