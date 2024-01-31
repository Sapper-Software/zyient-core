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

package io.zyient.base.common.utils;

import com.google.common.base.Preconditions;

public class RunUtils {
    public static final int INTERVAL_BASE_VALUE = 100;
    public static final int INTERVAL_MULTIPLIER = 20;

    public static void sleep(long interval) {
        if (interval <= 0) return;
        try {
            Thread.sleep(interval);
        } catch (InterruptedException ie) {
            // Do nothing...
        }
    }

    public static void wait(int intervalBase, int intervalMultiplier, int tryCount) {
        long duration = (long) Math.pow(intervalMultiplier, tryCount) + intervalBase;
        sleep(duration);
    }

    public static void wait(int tryCount) {
        wait(INTERVAL_BASE_VALUE, INTERVAL_MULTIPLIER, tryCount);
    }

    public static BackoffWait create(int retries) {
        return new BackoffWait(INTERVAL_BASE_VALUE, INTERVAL_MULTIPLIER, retries);
    }

    public static class BackoffWait {
        private final int intervalBase;
        private final int multiplier;
        private final int retryCount;

        private int count;


        public BackoffWait(int intervalBase, int multiplier, int retryCount) {
            Preconditions.checkArgument(intervalBase > 0);
            Preconditions.checkArgument(multiplier > 0);
            Preconditions.checkArgument(retryCount >= 0);
            this.intervalBase = intervalBase;
            this.multiplier = multiplier;
            this.retryCount = retryCount;
        }

        public boolean check() {
            if (count < retryCount) {
                RunUtils.wait(intervalBase, multiplier, count);
                count++;
                return true;
            }
            return false;
        }
    }
}
