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

package io.zyient.cdc.entity.model;

import lombok.Getter;
import lombok.experimental.Accessors;

public class AvroChangeType {
    private static final int OP_SCHEMA_CREATE = 0;
    private static final int OP_SCHEMA_UPDATE = 1;
    private static final int OP_SCHEMA_DELETE = 2;
    private static final int OP_DATA_INSERT = 3;
    private static final int OP_DATA_UPDATE = 4;
    private static final int OP_DATA_DELETE = 5;

    @Getter
    @Accessors(fluent = true)
    public enum EChangeType {
        EntityCreate(OP_SCHEMA_CREATE),
        EntityUpdate(OP_SCHEMA_UPDATE),
        EntityDelete(OP_SCHEMA_DELETE),
        RecordInsert(OP_DATA_INSERT),
        RecordUpdate(OP_DATA_UPDATE),
        RecordDelete(OP_DATA_DELETE);

        private final int opCode;

        EChangeType(int opCode) {
            this.opCode = opCode;
        }

        public static boolean isSchemaChange(EChangeType type) {
            return (type == EntityCreate || type == EntityDelete || type == EntityUpdate);
        }
    }
}
