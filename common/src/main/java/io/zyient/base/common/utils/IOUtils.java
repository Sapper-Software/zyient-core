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
import io.zyient.base.common.model.EReaderType;
import org.apache.commons.io.FileUtils;

import javax.annotation.Nonnull;
import java.io.*;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.util.Collection;

/**
 * Utility methods to perform IO.
 */
public class IOUtils {
    private static final String DEFAULT_TEMP_DIR_NAME = "zyient";
    private static final String NON_ASCII_RANGE = "[^\\x00-\\x7F]";

    /**
     * Check if the URI represent a local file (file://...)
     *
     * @param uri - URI to check.
     * @return - Is local file?
     */
    public static boolean isLocalFile(@Nonnull URI uri) {
        EReaderType type = EReaderType.parseFromUri(uri);
        if (type != null && type == EReaderType.File) {
            return true;
        }
        return false;
    }

    /**
     * Check if the directory exists, if not create.
     *
     * @param path - Directory path.
     * @return - Created or Exists?
     */
    public static boolean checkDirectory(String path) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        File file = new File(path);
        if (file.exists()) {
            if (file.isDirectory()) {
                return true;
            }
            return false;
        }
        return file.mkdirs();
    }

    /**
     * Check if the parent directory exists, if not create.
     *
     * @param path - File Path
     * @return - Created or Exists?
     */
    public static boolean checkParentDirectory(String path) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        File file = new File(path);
        if (file.exists()) {
            if (file.isFile()) {
                return true;
            }
            return false;
        }
        return checkDirectory(file.getParent());
    }

    /**
     * Convert the passed URI to a local path.
     *
     * @param uri - Remote URI.
     * @return - Local File Path.
     */
    public static String urlToLocalFilePath(@Nonnull URI uri) {
        EReaderType type = EReaderType.parseFromUri(uri);
        if (type != null && type != EReaderType.File) {
            String host = uri.getHost();
            String path = uri.getPath();
            String query = uri.getQuery();
            StringBuffer buffer = new StringBuffer();
            if (!Strings.isNullOrEmpty(host)) {
                host = replacePathString(host);
                buffer.append(host);
            }
            if (!Strings.isNullOrEmpty(path)) {
                if (buffer.length() > 0) {
                    buffer.append("/");
                }
                path = replacePathString(path);
                buffer.append(path);
            }
            if (!Strings.isNullOrEmpty(query)) {
                if (buffer.length() > 0) {
                    buffer.append("/");
                }
                query = replacePathString(query);
                buffer.append(query);
            }
            return buffer.toString();
        }
        return null;
    }

    /**
     * Replace whitespaces and non-ascii chars in the input string.
     *
     * @param input - Input String
     * @return - Replaced String.
     */
    private static String replacePathString(String input) {
        input = input.replaceAll(NON_ASCII_RANGE, "_");
        input = input.replaceAll("[\\s=.]", "_");

        return input;
    }

    /**
     * Utility method to copy a file to a destination folder.
     *
     * @param source  - Source File handle.
     * @param destDir - Destination directory.
     * @throws IOException
     */
    public static void copyFile(File source, File destDir) throws IOException {
        FileChannel sourceChannel = null;
        FileChannel destChannel = null;
        String destFile =
                String.format("%s/%s", destDir.getAbsolutePath(), source.getName());
        File dest = new File(destFile);
        try {
            sourceChannel = new FileInputStream(source).getChannel();
            destChannel = new FileOutputStream(dest).getChannel();
            destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
        } finally {
            sourceChannel.close();
            destChannel.close();
        }
    }

    public static String readContent(@Nonnull File file) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = null;
            StringBuilder builder = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                builder.append(line).append("\n");
            }
            if (builder.length() > 0) {
                return builder.toString();
            }
        }
        return null;
    }

    public static void cleanDirectory(@Nonnull File path, boolean recursive) throws IOException {
        Collection<File> files = FileUtils.listFiles(path, null, recursive);
        if (files != null && !files.isEmpty()) {
            for (File file : files) {
                if (file.isDirectory()) {
                    if (recursive)
                        cleanDirectory(file, recursive);
                }
                if (!file.delete()) {
                    DefaultLogger.debug(String.format("Failed to delete. [path=%s]", file.getAbsolutePath()));
                }
            }
        }
    }
}
