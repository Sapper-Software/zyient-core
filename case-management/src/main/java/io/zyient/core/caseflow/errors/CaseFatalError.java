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

package io.zyient.core.caseflow.errors;

public class CaseFatalError extends RuntimeException {
    private static final String __PREFIX = "Case Fatal Error : %s";

    public CaseFatalError(String message) {
        super(String.format(__PREFIX, message));
    }

    public CaseFatalError(String message, Throwable cause) {
        super(String.format(__PREFIX, message), cause);
    }

    public CaseFatalError(Throwable cause) {
        super(String.format(__PREFIX, cause.getLocalizedMessage()), cause);
    }
}
