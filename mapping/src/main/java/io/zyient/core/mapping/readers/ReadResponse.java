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

package io.zyient.core.mapping.readers;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.core.mapping.model.RecordResponse;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ReadResponse {
    private int recordCount = 0;
    private int errorCount = 0;
    private int commitCount = 0;
    private List<RecordResponse> records;

    public void add(@NonNull RecordResponse record) {
        if (records == null) {
            records = new ArrayList<>();
        }
        records.add(record);
    }

    public ReadResponse incrementCount() {
        recordCount++;
        return this;
    }

    public ReadResponse incrementErrorCount() {
        errorCount++;
        return this;
    }

    public ReadResponse incrementCommitCount() {
        commitCount++;
        return this;
    }
}
