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

package ai.sapper.cdc.entity.types;

import ai.sapper.cdc.entity.schema.SchemaEntity;
import lombok.NonNull;

public class BadEntityRecordError extends Exception {
    private static final String __PREFIX = "Invalid Entity Record: %s. [entity=%s]";

    public BadEntityRecordError(@NonNull SchemaEntity schemaEntity,
                                @NonNull String msg) {
        super(String.format(__PREFIX, msg, schemaEntity.toString()));
    }

    public BadEntityRecordError(@NonNull SchemaEntity schemaEntity,
                                @NonNull String msg,
                                @NonNull Throwable t) {
        super(String.format(__PREFIX, msg, schemaEntity.toString()), t);
    }
}
