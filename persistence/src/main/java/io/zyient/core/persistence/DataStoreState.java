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

package io.zyient.core.persistence;

import io.zyient.base.common.AbstractState;
import lombok.NonNull;

public class DataStoreState extends AbstractState<DataStoreState.EDataStoreState> {
    public DataStoreState() {
        super(EDataStoreState.Error, EDataStoreState.Unknown);
        setState(EDataStoreState.Unknown);
    }

    public enum EDataStoreState {
        Unknown, Initialized, Available, Closed, Error
    }

    public boolean isAvailable() {
        return getState() == EDataStoreState.Available;
    }

    public void check(@NonNull EDataStoreState state) throws DataStoreException {
        if (getState() != state) {
            throw new DataStoreException(
                    String.format("DataStore state is invalid. [expected=%s][state=%s]",
                            state.name(), getState().name()));
        }
    }
}
