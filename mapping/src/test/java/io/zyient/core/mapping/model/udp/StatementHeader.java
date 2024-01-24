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

package io.zyient.core.mapping.model.udp;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class StatementHeader {
    private List<Month> months;
    private List<Integer> years;

    public void setMonthsFrom(List<String> months) throws Exception {
        if (months != null && !months.isEmpty()) {
            if (this.months == null) {
                this.months = new ArrayList<>();
            }
            for (String m : months) {
                Month month = Month.parse(m);
                if (month == null) {
                    throw new Exception(String.format("Invalid month value: [%s]", m));
                }
            }
        }
    }
}
