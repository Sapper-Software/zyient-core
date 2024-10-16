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

package io.zyient.core.mapping.mapper;

import io.zyient.core.mapping.annotations.EntityRef;
import io.zyient.core.mapping.model.Holding;
import io.zyient.core.mapping.model.MappedResponse;

import java.util.Map;

@EntityRef(type = Holding.class)
public class HoldingMappedResponse extends MappedResponse<Holding> {
    public HoldingMappedResponse(Map<String, Object> source) {
        super(source);
    }
}
