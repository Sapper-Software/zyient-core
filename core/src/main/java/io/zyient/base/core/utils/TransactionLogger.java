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

package io.zyient.base.core.utils;

import io.zyient.base.common.utils.DefaultLogger;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionLogger extends DefaultLogger {
    private static final Logger LOG = LoggerFactory.getLogger("ai.sapper");

    public void debug(@NonNull Class<?> caller, Object txId, String mesg) {
        LOG.debug(String.format("[TXID=%s][%s]: %s", txId, caller.getCanonicalName(), mesg));
    }

    public void info(@NonNull Class<?> caller, Object txId, String mesg) {
        LOG.info(String.format("[TXID=%s][%s]: %s", txId, caller.getCanonicalName(), mesg));
    }

    public void warn(@NonNull Class<?> caller, Object txId, String mesg) {
        LOG.warn(String.format("[TXID=%s][%s]: %s", txId, caller.getCanonicalName(), mesg));
    }

    public void error(@NonNull Class<?> caller, Object txId, String mesg) {
        LOG.error(String.format("[TXID=%s][%s]: %s", txId, caller.getCanonicalName(), mesg));
    }

    public void error(Class<?> caller, Object txId, Throwable t) {
        LOG.error(String.format("[TXID=%s][%s]: %s", txId, caller.getCanonicalName(), t.getLocalizedMessage()));
        if (LOG.isDebugEnabled()) {
            DefaultLogger.stacktrace(t);
        }
    }

    public void error(@NonNull Class<?> caller, Object txId, String mesg, Throwable t) {
        LOG.error(String.format("[TXID=%s][%s]: %s", txId, caller.getCanonicalName(), mesg), t);
        if (LOG.isDebugEnabled()) {
            DefaultLogger.stacktrace(t);
        }
    }

    public static final TransactionLogger LOGGER = new TransactionLogger();
}
