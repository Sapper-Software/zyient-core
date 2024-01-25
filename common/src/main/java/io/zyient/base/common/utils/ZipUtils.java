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
import com.google.common.base.Strings;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.EncryptionMethod;

import javax.annotation.Nonnull;
import java.io.File;
import java.security.PrivilegedActionException;

public class ZipUtils {
    public static class ZipException extends Exception {
        private static final String __PREFIX = "Data Sink Error : %s";

        /**
         * Constructs a new exception with the specified detail message.  The
         * cause is not initialized, and may subsequently be initialized by
         * a call to {@link #initCause}.
         *
         * @param message the detail message. The detail message is saved for
         *                later retrieval by the {@link #getMessage()} method.
         */
        public ZipException(String message) {
            super(String.format(__PREFIX, message));
        }

        /**
         * Constructs a new exception with the specified detail message and
         * cause.  <p>Note that the detail message associated with
         * {@code cause} is <i>not</i> automatically incorporated in
         * this exception's detail message.
         *
         * @param message the detail message (which is saved for later retrieval
         *                by the {@link #getMessage()} method).
         * @param cause   the cause (which is saved for later retrieval by the
         *                {@link #getCause()} method).  (A <tt>null</tt> value is
         *                permitted, and indicates that the cause is nonexistent or
         *                unknown.)
         * @since 1.4
         */
        public ZipException(String message, Throwable cause) {
            super(String.format(__PREFIX, message), cause);
        }

        /**
         * Constructs a new exception with the specified cause and a detail
         * message of <tt>(cause==null ? null : cause.toString())</tt> (which
         * typically contains the class and detail message of <tt>cause</tt>).
         * This constructor is useful for exceptions that are little more than
         * wrappers for other throwables (for example, {@link
         * PrivilegedActionException}).
         *
         * @param cause the cause (which is saved for later retrieval by the
         *              {@link #getCause()} method).  (A <tt>null</tt> value is
         *              permitted, and indicates that the cause is nonexistent or
         *              unknown.)
         * @since 1.4
         */
        public ZipException(Throwable cause) {
            super(String.format(__PREFIX, cause.getLocalizedMessage()), cause);
        }
    }

    public static String zip(@Nonnull String dir2zip, @Nonnull String outFielPath) throws ZipException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(outFielPath));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dir2zip));
        try {
            ZipFile zf = new ZipFile(new File(outFielPath));
            zf.addFolder(new File(dir2zip));
            return zf.getFile().getAbsolutePath();
        } catch (Exception e) {
            throw new ZipException(e);
        }
    }

    public static String zip(@Nonnull String dir2zip, @Nonnull String outFielPath,
                             @Nonnull String password) throws ZipException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(outFielPath));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dir2zip));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));
        try {
            ZipParameters zipParameters = new ZipParameters();
            zipParameters.setEncryptFiles(true);
            zipParameters.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);

            try (ZipFile zf = new ZipFile(new File(outFielPath), password.toCharArray())) {
                zf.addFolder(new File(dir2zip), zipParameters);
                return zf.getFile().getAbsolutePath();
            }
        } catch (Exception e) {
            throw new ZipException(e);
        }
    }

    public static void unzip(@Nonnull String zipFilePath, @Nonnull String destDir) throws ZipException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(zipFilePath));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(destDir));

        File dir = new File(destDir);
        // create output directory if it doesn't exist
        if (!dir.exists()) dir.mkdirs();

        try {
            try (ZipFile zf = new ZipFile(new File(zipFilePath))) {
                zf.extractAll(destDir);
            }
        } catch (Exception e) {
            throw new ZipException(e);
        }
    }

    public static void unzip(@Nonnull String zipFilePath,
                             @Nonnull String destDir,
                             @Nonnull String password) throws ZipException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(zipFilePath));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(destDir));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));

        File dir = new File(destDir);
        // create output directory if it doesn't exist
        if (!dir.exists()) dir.mkdirs();

        try {
            try (ZipFile zf = new ZipFile(new File(zipFilePath), password.toCharArray())) {
                zf.extractAll(destDir);
            }
        } catch (Exception e) {
            throw new ZipException(e);
        }
    }
}
