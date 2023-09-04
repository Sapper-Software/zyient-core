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

package io.zyient.base.core.stores;

import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.core.model.BaseEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.*;

@Getter
@Accessors(fluent = true)
public class JsonReference {
    private final Map<IKey, EEntityState> reference = new HashMap<>();

    public <T extends IEntity<?>> JsonReference addAll(@NonNull Collection<T> records) {
        reference.clear();
        for (T record : records) {
            EEntityState rstate = null;
            if (record instanceof BaseEntity<?>) {
                rstate = ((BaseEntity<?>) record).getState().getState();
            } else {
                rstate = EEntityState.Synced;
            }
            reference.put(record.getKey(), rstate);
        }
        return this;
    }

    public <T extends IEntity<?>> JsonReference update(@NonNull Collection<T> records) {
        for (T record : records) {
            IKey key = record.getKey();
            EEntityState rstate = state(key);
            if (record instanceof BaseEntity<?>) {
                rstate = ((BaseEntity<?>) record).getState().getState();
            } else if (rstate == null) {
                rstate = EEntityState.New;
            }
            reference.put(key, rstate);
        }
        if (reference.size() > records.size()) {
            List<IKey> toDelete = new ArrayList<>();
            for (IKey key : reference.keySet()) {
                if (find(records, key) == null) {
                    toDelete.add(key);
                }
            }
            if (!toDelete.isEmpty()) {
                for (IKey key : toDelete) {
                    reference.put(key, EEntityState.Deleted);
                }
            }
        }
        return this;
    }

    private <T extends IEntity<?>> T find(Collection<T> records, IKey key) {
        for (T record : records) {
            if (key.compareTo(record.getKey()) == 0) {
                return record;
            }
        }
        return null;
    }

    public <K, T extends IEntity<?>> JsonReference addAll(@NonNull Map<K, T> records) {
        reference.clear();
        for (K k : records.keySet()) {
            T record = records.get(k);
            EEntityState rstate = null;
            if (record instanceof BaseEntity<?>) {
                rstate = ((BaseEntity<?>) record).getState().getState();
            } else {
                rstate = EEntityState.Synced;
            }
            reference.put(record.getKey(), rstate);
        }
        return this;
    }

    public <K, T extends IEntity<?>> JsonReference update(@NonNull Map<K, T> records) {
        for (K k : records.keySet()) {
            T record = records.get(k);
            IKey key = record.getKey();
            EEntityState rstate = state(key);
            if (record instanceof BaseEntity<?>) {
                rstate = ((BaseEntity<?>) record).getState().getState();
            } else if (rstate == null) {
                rstate = EEntityState.New;
            }
            reference.put(key, rstate);
        }
        if (reference.size() > records.size()) {
            Collection<T> values = records.values();
            List<IKey> toDelete = new ArrayList<>();
            for (IKey key : reference.keySet()) {
                if (find(values, key) == null) {
                    toDelete.add(key);
                }
            }
            if (!toDelete.isEmpty()) {
                for (IKey key : toDelete) {
                    reference.put(key, EEntityState.Deleted);
                }
            }
        }
        return this;
    }

    public EEntityState state(@NonNull IKey key) {
        return reference.get(key);
    }

    public boolean update(@NonNull IKey key, EEntityState state) {
        if (reference.containsKey(key)) {
            reference.put(key, state);
            return true;
        }
        return false;
    }

    public void add(@NonNull IKey key, EEntityState state) {
        reference.put(key, state);
    }

    public void add(@NonNull IEntity<?> entity, EEntityState state) {
        if (entity instanceof BaseEntity<?>) {
            state = ((BaseEntity<?>) entity).getState().getState();
        }
        add(entity.getKey(), state);
    }

    public int removeDeleted() {
        List<IKey> toDelete = new ArrayList<>();
        for (IKey key : reference().keySet()) {
            if (reference.get(key) == EEntityState.Deleted) {
                toDelete.add(key);
            }
        }
        if (!toDelete.isEmpty()) {
            for (IKey key : toDelete) {
                reference.remove(key);
            }
        }
        return toDelete.size();
    }

    public boolean remove(@NonNull IKey key) {
        return reference.remove(key) != null;
    }
}
