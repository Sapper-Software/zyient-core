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

package io.zyient.base.core.state;

import com.google.common.base.Preconditions;
import io.zyient.base.common.utils.DefaultLogger;
import lombok.NonNull;

public class HeartbeatThread implements Runnable {
    private final String name;
    private final long sleepInterval; // 60 secs.
    private BaseStateManager stateManager;
    private boolean running = true;

    public HeartbeatThread(@NonNull String name, long sleepInterval) {
        this.name = name;
        this.sleepInterval = sleepInterval;
    }

    public HeartbeatThread withStateManager(@NonNull BaseStateManager stateManager) {
        this.stateManager = stateManager;
        return this;
    }

    public void terminate() {
        running = false;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        Preconditions.checkNotNull(stateManager);
        try {
            while (running) {
                stateManager.heartbeat(stateManager.moduleInstance().getInstanceId());
                Thread.sleep(sleepInterval);
            }
        } catch (Exception ex) {
            DefaultLogger.error(
                    String.format("Heartbeat thread terminated. [module=%s]",
                            stateManager.moduleInstance().getModule()), ex);
        }
    }
}
