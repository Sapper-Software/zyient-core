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

package ai.sapper.cdc.core;

import ai.sapper.cdc.common.AbstractState;
import lombok.NonNull;

public interface Service<E extends Enum<?>> {
    Service<E> setConfigFile(@NonNull String path);

    Service<E> setConfigSource(@NonNull String type);

    Service<E> init() throws Exception;

    Service<E> start() throws Exception;

    Service<E> stop() throws Exception;

    AbstractState<E> status();

    String name();
}
