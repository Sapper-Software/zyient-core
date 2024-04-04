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

package io.zyient.core.filesystem.sync.azure.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.state.Offset;
import io.zyient.core.filesystem.sync.EEventProcessorState;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public class AzureFSEventOffset extends Offset {
    private String name;
    private EEventProcessorState state;
    private long createTime;
    private long updateTime;
    private String token;

    @Override
    public String asString() {
        try {
            return JSONUtils.asString(this);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Offset fromString(@NonNull String source) throws Exception {
        AzureFSEventOffset offset = JSONUtils.read(source, getClass());
        name = offset.name;
        state = offset.state;
        createTime = offset.createTime;
        updateTime = offset.updateTime;
        token = offset.token;
        return this;
    }

    @Override
    public int compareTo(@NonNull Offset offset) {
        if (offset instanceof AzureFSEventOffset) {
            int ret = name.compareTo(((AzureFSEventOffset) offset).name);
            if (ret == 0) {
                if (Strings.isNullOrEmpty(token)) {
                    if (!Strings.isNullOrEmpty(((AzureFSEventOffset) offset).token)) {
                        ret = Short.MAX_VALUE;
                    }
                } else
                    ret = token.compareTo(((AzureFSEventOffset) offset).token);
            }
            return ret;
        }
        throw new RuntimeException(String.format("Invalid offset type. [type=%s]",
                offset.getClass().getCanonicalName()));
    }
}
