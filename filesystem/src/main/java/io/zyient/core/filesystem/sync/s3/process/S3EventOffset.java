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

package io.zyient.core.filesystem.sync.s3.process;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.state.Offset;
import io.zyient.core.filesystem.sync.s3.model.S3EventType;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public class S3EventOffset extends Offset {
    private String messageId;
    private String bucket;
    private String key;
    private S3EventType eventType;

    public S3EventOffset() {
    }

    public S3EventOffset(@NonNull S3EventOffset source) {
        super(source);
        messageId = source.messageId;
        bucket = source.bucket;
        key = source.key;
        eventType = source.eventType;
    }

    @Override
    public String asString() {
        try {
            return JSONUtils.asString(this);
        } catch (Exception ex) {
            DefaultLogger.error(ex.getLocalizedMessage());
        }
        return null;
    }

    @Override
    public Offset fromString(@NonNull String source) throws Exception {
        S3EventOffset offset = JSONUtils.read(source, getClass());
        setTimeUpdated(offset.getTimeUpdated());
        messageId = offset.messageId;
        bucket = offset.bucket;
        key = offset.key;
        eventType = offset.eventType;
        return this;
    }

    @Override
    public int compareTo(@NonNull Offset offset) {
        if (offset instanceof S3EventOffset) {
            return messageId.compareTo(((S3EventOffset) offset).messageId);
        }
        throw new RuntimeException(String.format("Invalid offset type. [type=%s]",
                offset.getClass().getCanonicalName()));
    }
}
