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

package io.zyient.base.common.utils;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class DefaultLogger {
    private static Logger LOGGER = LoggerFactory.getLogger(DefaultLogger.class);

    public static void setDefaultLogger(@NonNull Logger logger) {
        LOGGER = logger;
    }

    public static boolean isDebugEnabled() {
        return LOGGER.isDebugEnabled();
    }

    public static boolean isTraceEnabled() {
        return LOGGER.isTraceEnabled();
    }

    public static boolean isGreaterOrEqual(Level l1, Level l2) {
        int i1 = l1.toInt();
        int i2 = l2.toInt();
        return (i1 >= i2);
    }

    public static String error(Throwable err, String format, Object... args) {
        String mesg = String.format(format, args);
        return String.format("%s : ERROR : %s\n", mesg, err.getLocalizedMessage());
    }

    public static String getStacktrace(@NonNull Throwable error) {
        StringBuilder buff = new StringBuilder(String.format("ERROR: %s", error.getLocalizedMessage()));
        buff.append("\n\t********************************BEGIN TRACE********************************\n");
        Throwable e = error;
        while (e != null) {
            buff.append("\n\t---------------------------------------------------------------------------\n");
            buff.append(String.format("\tERROR: %s\n", e.getLocalizedMessage()));
            for (StackTraceElement se : e.getStackTrace()) {
                buff.append(String.format("\t%s\n", se.toString()));
            }
            e = e.getCause();
        }
        buff.append("\t********************************END   TRACE********************************\n");

        return buff.toString();
    }

    public static void stacktrace(@NonNull Throwable error) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(getStacktrace(error));
        }
    }

    public static void stacktrace(Logger logger, @NonNull Throwable error) {
        if (logger == null) {
            logger = LOGGER;
        }
        if (logger.isDebugEnabled()) {
            logger.debug(getStacktrace(error));
        }
    }

    public static void error(@NonNull String msg) {
        LOGGER.error(msg);
    }

    public static void error(Logger logger, String msg) {
        if (logger == null) {
            logger = LOGGER;
        }
        logger.error(msg);
    }

    public static void error(String msg, @NonNull Throwable error) {
        LOGGER.error(msg, error);
    }

    public static void error(Logger logger, String msg, @NonNull Throwable error) {
        if (logger == null) {
            logger = LOGGER;
        }
        logger.error(msg, error);
    }

    public static void warn(String msg) {
        LOGGER.warn(msg);
    }

    public static void warn(Logger logger, String msg) {
        if (logger == null) {
            logger = LOGGER;
        }
        logger.warn(msg);
    }

    public static void warn(String msg, @NonNull Throwable error) {
        LOGGER.warn(msg, error);
    }

    public static void warn(Logger logger, String msg, @NonNull Throwable error) {
        if (logger == null) {
            logger = LOGGER;
        }
        logger.warn(msg, error);
    }

    public static void info(String msg) {
        LOGGER.info(msg);
    }

    public static void info(Logger logger, String msg) {
        if (logger == null) {
            logger = LOGGER;
        }
        logger.info(msg);
    }

    public static void debug(String msg, @NonNull Throwable error) {
        LOGGER.debug(msg, error);
    }

    public static void debug(Logger logger, String msg, @NonNull Throwable error) {
        if (logger == null) {
            logger = LOGGER;
        }
        logger.debug(msg, error);
    }

    public static void debug(String msg) {
        LOGGER.debug(msg);
    }

    public static void debug(Logger logger, String msg) {
        if (logger == null) {
            logger = LOGGER;
        }
        logger.debug(msg);
    }

    public static void trace(String msg) {
        trace(LOGGER, msg);
    }

    public static void trace(@NonNull Object data) {
        trace(LOGGER, data);
    }

    public static void trace(Logger logger, @NonNull Object data) {
        if (logger == null) {
            logger = LOGGER;
        }
        try {
            String msg = JSONUtils.asString(data, data.getClass());
            logger.trace(msg);
        } catch (Exception ex) {
            logger.error(ex.getLocalizedMessage());
        }
    }

    public static void trace(Logger logger, String msg) {
        if (logger == null) {
            logger = LOGGER;
        }
        logger.trace(msg);
    }
}
