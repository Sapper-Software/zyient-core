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

package io.zyient.intake.flow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Data;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

@Data
public class TaskExecutionRecord<T> {
    private long startTime;
    private long endTime;
    private T record;
    private Map<String, TaskResponse> responses = new HashMap<>();
    @JsonIgnore
    private TaskContext context;

    public void addResponse(@Nonnull String name, @Nonnull TaskResponse response) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        responses.put(name, response);
    }
}
