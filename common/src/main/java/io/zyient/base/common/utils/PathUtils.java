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

import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

public class PathUtils {
    public static final String TEMP_PATH = String.format("%s/zyient/framework/temp", System.getProperty("java.io.tmpdir"));

    private static final Pattern PAT_FILENAME_REMOVE_INVALID_CHARACTERS = Pattern.compile("[<>:\"/\\\\|?*\\x00-\\x1F]");
    private static final Pattern PAT_FILENAME_REMOVE_LEADING_CHARACTERS = Pattern.compile("^[\\s.]+([^\\s.].*)$");
    private static final Pattern PAT_FILENAME_REMOVE_TRAILING_CHARACTERS = Pattern.compile("^(.*[^\\s.])[\\s.]+$");
    private static final Pattern PAT_FILENAME_TRIM = Pattern.compile("^[\\s.]*$");
    private static final String DEFAULT_FILENAME = "_";

    /**
     * Validate a file name based on the <tt>filename</tt> by removing illegal characters, leading/trailing
     * dots/whitespace and omitting the file name being one of the reserved file names.
     * <p>
     * Never returns an empty String or null.
     * <p>
     * Chops filename to max length of 250 characters.
     *
     * @since 5.1
     */
    public static String toValidFilename(final String filename) {
        if (filename == null) {
            return DEFAULT_FILENAME;
        }
        String s = filename;
        s = PAT_FILENAME_REMOVE_INVALID_CHARACTERS.matcher(s).replaceAll("");
        try {
            Paths.get(s);
            //ok
        } catch (InvalidPathException ex) { // NOSONAR
            //nok, check every character in sandwich test
            StringBuilder buf = new StringBuilder();
            for (char ch : s.toCharArray()) {
                try {
                    Paths.get("a" + ch + "a");
                    buf.append(ch);
                } catch (InvalidPathException ex2) { // NOSONAR
                    //skip ch
                }
            }
            s = buf.toString();
        }

        //remove leading and trailing dots, whitespace
        s = PAT_FILENAME_REMOVE_LEADING_CHARACTERS.matcher(s).replaceAll("$1");
        s = PAT_FILENAME_REMOVE_TRAILING_CHARACTERS.matcher(s).replaceAll("$1");
        s = PAT_FILENAME_TRIM.matcher(s).replaceAll("");

        if (s.isEmpty()) {
            return DEFAULT_FILENAME;
        }

        // on some operating systems, the name may not be longer than 250 characters
        if (s.length() > 250) {
            int dot = s.lastIndexOf('.');
            String suffix = (dot > 0 ? s.substring(dot) : "");
            //suffix is at most 32 chars
            if (suffix.length() > 32) {
                suffix = "";
            }
            s = s.substring(0, 250 - suffix.length()) + suffix;
        }

        return s;
    }

    /**
     * Is the given <tt>filename</tt> a valid file name
     * <p>
     * Uses {@link #toValidFilename(String)} to check
     *
     * @return <tt>false</tt> if <tt>filename</tt> is not a valid filename<br/>
     * <tt>true</tt> if <tt>filename</tt> is a valid filename
     */
    public static boolean isValidFilename(String filename) {
        return toValidFilename(filename).equals(filename);
    }

    /**
     * @return Returns <code>true</code> if the given file is a zip file, otherwise <code>false</code>.
     */
    public static boolean isZipFile(File file) {
        if (file == null || file.isDirectory() || !file.canRead() || file.length() < 4) {
            return false;
        }
        try (
                @SuppressWarnings("squid:S2095")
                DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            int test = in.readInt();
            return test == 0x504b0304; // magic number of a zip file
        } catch (IOException e) { // NOSONAR
        }
        return false;
    }

    public static String formatPath(@NonNull String path) {
        path = path.replaceAll("\\\\", "/");
        path = path.replaceAll("/\\s*/", "/");
        return path;
    }

    public static File getTempDir() throws IOException {
        File dir = new File(TEMP_PATH);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException(
                        String.format("Failed to create temp directory. [path=%s]", dir.getAbsolutePath()));
            }
        }
        return dir;
    }

    public static File getTempDir(@NonNull String name) throws IOException {
        File dir = new File(formatPath(String.format("%s/%s", TEMP_PATH, name)));
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException(
                        String.format("Failed to create temp directory. [path=%s]", dir.getAbsolutePath()));
            }
        }
        return dir;
    }

    public static File getTempFile(@NonNull String name,
                                   @NonNull String ext) throws IOException {
        File dir = getTempDir();
        File path = new File(String.format("%s/%s.%s",
                dir.getAbsolutePath(), name, ext));
        if (path.exists()) {
            if (!path.delete()) {
                throw new IOException(String.format("Failed to delete temp file. [path=%s]", path.getAbsolutePath()));
            }
        }
        return path;
    }

    public static File getTempFile(@NonNull String name) throws IOException {
        File dir = getTempDir();
        File path = new File(String.format("%s/%s",
                dir.getAbsolutePath(), name));
        if (path.exists()) {
            if (!path.delete()) {
                throw new IOException(String.format("Failed to delete temp file. [path=%s]", path.getAbsolutePath()));
            }
        }
        return path;
    }

    public static File getTempFile() throws IOException {
        return getTempFile(UUID.randomUUID().toString(), "tmp");
    }

    public static File getTempFileWithName(@NonNull String name) throws IOException {
        File dir = getTempDir();
        File path = new File(String.format("%s/%s",
                dir.getAbsolutePath(), name));
        if (path.exists()) {
            if (!path.delete()) {
                throw new IOException(String.format("Failed to delete temp file. [path=%s]", path.getAbsolutePath()));
            }
        }
        return path;
    }

    public static File getTempFileWithExt(@NonNull String ext) throws IOException {
        return getTempFile(UUID.randomUUID().toString(), ext);
    }

    public static class ZkPathBuilder {
        private final List<String> paths = new ArrayList<>();

        public ZkPathBuilder() {
        }

        public ZkPathBuilder(@NonNull String base) {
            paths.add(base);
        }

        public ZkPathBuilder withPath(@NonNull String path) {
            paths.add(path);
            return this;
        }

        public String build() {
            if (!paths.isEmpty()) {
                StringBuilder fmt = new StringBuilder();
                for (String path : paths) {
                    fmt.append("/").append(path);
                }
                return formatZkPath(fmt.toString());
            }
            return null;
        }

        public static String formatZkPath(@NonNull String path) {
            path = path.replaceAll("/\\s*/", "/");
            if (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
            return path;
        }
    }

    public static File readFile(@NonNull String path) throws Exception {
        URI uri = new URI(path);
        String schema = uri.getScheme();
        File file = null;
        if (Strings.isNullOrEmpty(schema)) {
            file = new File(path);
            if (!file.exists()) {
                throw new IOException(String.format("Specified path not found. [path=%s]",
                        file.getAbsolutePath()));
            }
        } else if (schema.compareToIgnoreCase("file") == 0) {
            path = String.format("%s/%s", uri.getHost(), uri.getPath());
            file = new File(path);
            if (!file.exists()) {
                throw new IOException(String.format("Specified path not found. [path=%s]",
                        file.getAbsolutePath()));
            }
        } else {
            URL url = uri.toURL();
            String ext = FilenameUtils.getExtension(path);
            String name = UUID.randomUUID().toString();
            if (Strings.isNullOrEmpty(ext))
                file = getTempFile(name);
            else
                file = getTempFile(name, ext);
            try (InputStream in = url.openStream()) {
                ReadableByteChannel rbc = Channels.newChannel(in);
                try (FileOutputStream fos = new FileOutputStream(file)) {
                    fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
                }
            }
        }
        return file;
    }
}
