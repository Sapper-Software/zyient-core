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

package ai.sapper.cdc.common.threads;

import ai.sapper.cdc.common.utils.DefaultLogger;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;

@Getter
@Accessors(fluent = true)
public class ManagedThread extends Thread {
    private final ThreadState state = new ThreadState();
    private final Set<IThreadListener> fIThreadListeners = new HashSet<>();
    private Runnable runnable;
    private final ThreadManager manager;

    public ManagedThread(@NonNull ThreadManager manager,
                         @NonNull Runnable target,
                         @NonNull String name) {
        super(target, name);
        runnable = target;
        this.manager = manager;
    }

    public ManagedThread(@NonNull ThreadManager manager,
                         @NonNull ThreadGroup group,
                         @NonNull Runnable target,
                         @NonNull String name) {
        super(group, target, name);
        runnable = target;
        this.manager = manager;
    }

    public ManagedThread(@NonNull ThreadManager manager,
                         @NonNull ThreadGroup group,
                         @NonNull Runnable target,
                         @NonNull String name,
                         long stackSize) {
        super(group, target, name, stackSize);
        runnable = target;
        this.manager = manager;
    }

    public ManagedThread register(@Nonnull IThreadListener pIThreadListener) {
        fIThreadListeners.add(pIThreadListener);
        return this;
    }

    @Override
    public synchronized void start() {
        if (!fIThreadListeners.isEmpty()) {
            for (IThreadListener listener : fIThreadListeners) {
                listener.event(EThreadEvent.Start, this);
            }
        }
        super.start();
    }

    @Override
    public void run() {
        if (!fIThreadListeners.isEmpty()) {
            for (IThreadListener listener : fIThreadListeners) {
                listener.event(EThreadEvent.Run, this);
            }
        }
        try {
            super.run();
        } catch (Throwable t) {
            Class<?> type = getClass();
            if (runnable != null) {
                type = runnable.getClass();
            }
            errorEvent(type, t);
        } finally {
            if (!fIThreadListeners.isEmpty()) {
                for (IThreadListener listener : fIThreadListeners) {
                    listener.event(EThreadEvent.Stop, this);
                }
            }
            try {
                manager.remove(this.getName());
            } catch (Exception ex) {
                DefaultLogger.error(getClass().getCanonicalName(), ex);
            }
        }
    }

    @Override
    public void interrupt() {
        if (!fIThreadListeners.isEmpty()) {
            for (IThreadListener listener : fIThreadListeners) {
                listener.event(EThreadEvent.Interrupted, this);
            }
        }
        super.interrupt();
    }

    public void errorEvent(Class<?> type, Throwable error) {
        if (!fIThreadListeners.isEmpty()) {
            for (IThreadListener listener : fIThreadListeners) {
                listener.event(EThreadEvent.Error, this, type, error);
            }
        }
    }
}
