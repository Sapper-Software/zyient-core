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

package io.zyient.base.core.io.indexing;

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

public class InodeIndexConstants {
    public static final String NAME_FS_PATH = "fsPath";
    public static final String NAME_UUID = "UUID";
    public static final String NAME_DOMAIN = "domain";
    public static final String NAME_PATH = "path";
    public static final String NAME_ABSOLUTE_PATH = "absolutePath";
    public static final String NAME_TYPE = "type";
    public static final String NAME_PARENT_ZK_PATH = "zkPathParent";
    public static final String NAME_ZK_PATH = "zkPath";
    public static final String NAME_NAME = "name";
    public static final String NAME_ATTRS = "attributes";
    public static final String NAME_STATE = "state";
    public static final String NAME_CREATE_DATE = "created";
    public static final String NAME_MODIFIED_DATE = "modified";

    @Getter
    @Accessors(fluent = true)
    public enum InodeQuery {
        FS_PATH(NAME_FS_PATH),
        UUID(NAME_UUID),
        DOMAIN(NAME_DOMAIN),
        PATH(NAME_PATH),
        ABSOLUTE_PATH(NAME_ABSOLUTE_PATH),
        TYPE(NAME_TYPE),
        ZK_PATH(NAME_ZK_PATH),
        NAME(NAME_NAME),
        ATTRIBUTES(NAME_ATTRS),
        STATE(NAME_STATE),
        CREATED(NAME_CREATE_DATE),
        MODIFIED(NAME_MODIFIED_DATE);

        private final String term;

        InodeQuery(@NonNull String term) {
            this.term = term;
        }
    }
}
