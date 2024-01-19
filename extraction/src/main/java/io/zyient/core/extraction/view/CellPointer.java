/*
 * Copyright(C) (2024) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.core.extraction.view;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.zyient.base.common.json.ClassNameDeSerializer;
import io.zyient.base.common.json.ClassNameSerializer;
import io.zyient.core.extraction.model.Cell;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CellPointer {
    private String cellId;
    @JsonSerialize(keyUsing = ClassNameSerializer.class)
    @JsonDeserialize(using = ClassNameDeSerializer.class)
    private Class<? extends Cell<?>> cellType;
    private String value;
}
