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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DateTimeUtils;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import lombok.NonNull;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;

import javax.annotation.Nonnull;
import java.io.*;
import java.net.URLConnection;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedActionException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class FileUtils {
    public static final String FILE_EXT_EML = "eml";
    public static final String MIME_TYPE_ZIP = "application/zip";
    public static final String MIME_TYPE_BZIP = "application/x-bzip";
    public static final String MIME_TYPE_BZIP2 = "application/x-bzip2";
    public static final String MIME_TYPE_GZIP = "application/gzip";
    public static final String MIME_TYPE_RAR = "application/x-rar-compressed";
    public static final String MIME_TYPE_TAR = "application/x-tar";
    public static final String MIME_TYPE_7ZIP = "application/x-7z-compressed";
    public static final String[] ARCHIVE_TYPES = {
            MIME_TYPE_7ZIP,
            MIME_TYPE_BZIP,
            MIME_TYPE_BZIP2,
            MIME_TYPE_GZIP,
            MIME_TYPE_RAR,
            MIME_TYPE_TAR,
            MIME_TYPE_ZIP
    };
    public static final String MIME_TYPE_MSG = "application/vnd.ms-outlook";
    public static final String MIME_TYPE_EML = "message/rfc822";
    public static final String[] EMAIL_MIME_TYPES = {
            MIME_TYPE_EML
    };
    public static final String MIME_TYPE_JSON = "application/json";
    public static final String MIME_TYPE_PDF = "application/pdf";
    public static final String MIME_TYPE_TEXT = "text/plain";
    public static final String MIME_TYPE_HTML = "text/html";
    public static final String MIME_TYPE_CSV = "text/csv";
    public static final String MIME_TYPE_XML = "application/xml";
    public static final String[] XML_MIME_TYPES = {
            "application/xhtml+xml",
            "text/xml",
            MIME_TYPE_XML
    };
    public static final String[] MIME_TYPE_MS_WORD = {
            "application/msword",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    };
    public static final String[] MIME_TYPE_MS_EXCEL = {
            "application/vnd.ms-excel",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    };
    public static final String[] MIME_TYPE_MS_POWER_POINT = {
            "application/vnd.ms-powerpoint",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    };

    public static String detectURLMimeType(final File file) throws IOException {
        URLConnection connection = file.toURL().openConnection();
        String mimeType = connection.getContentType();
        DefaultLogger.debug(String.format("[file=%s][mimeType=%s]", file.getAbsolutePath(), mimeType));
        return mimeType;
    }

    public static String detectMimeType(@NonNull File path) throws Exception {
        TikaConfig config = TikaConfig.getDefaultConfig();
        Detector detector = config.getDetector();

        try (TikaInputStream stream = TikaInputStream.get(path.toPath())) {
            Metadata metadata = new Metadata();
            metadata.add(TikaCoreProperties.RESOURCE_NAME_KEY, path.getName());
            MediaType mediaType = detector.detect(stream, metadata);
            return mediaType.toString();
        }
    }

    public static String getFileMimeType(@Nonnull String filename) throws FileUtilsException {
        try {
            File fi = new File(filename);
            if (!fi.exists() || !fi.isFile()) {
                throw new FileUtilsException(String.format("Invalid input file. [path=%s]",
                        fi.getAbsolutePath()));
            }
            return detectMimeType(fi);
        } catch (Exception ex) {
            return "MIME/ERROR";
        }
    }

    public static boolean isExcelType(@NonNull String mimeType) {
        for (String mt : MIME_TYPE_MS_EXCEL) {
            if (mt.compareToIgnoreCase(mimeType) == 0) {
                return true;
            }
        }
        return false;
    }

    public static boolean isXmlType(@NonNull String mimeType) {
        for (String mt : XML_MIME_TYPES) {
            if (mt.compareToIgnoreCase(mimeType) == 0) {
                return true;
            }
        }
        return false;
    }

    public static boolean isArchiveFile(@Nonnull String filename) throws FileUtilsException {
        String mime = getFileMimeType(filename);
        if (!Strings.isNullOrEmpty(mime)) {
            mime = mime.trim().toLowerCase();
            if (!Strings.isNullOrEmpty(mime)) {
                for (String ft : ARCHIVE_TYPES) {
                    if (ft.compareTo(mime) == 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static boolean isEmailFile(@Nonnull String filename) throws FileUtilsException {
        String mime = getFileMimeType(filename);
        mime = mime.trim().toLowerCase();
        if (!Strings.isNullOrEmpty(mime)) {
            for (String mt : EMAIL_MIME_TYPES) {
                if (mt.compareTo(mime) == 0) {
                    return true;
                }
            }
            if (mime.compareToIgnoreCase(MIME_TYPE_TEXT) == 0) {
                String ext = FilenameUtils.getExtension(filename);
                if (!Strings.isNullOrEmpty(ext)) {
                    if (ext.trim().toLowerCase().compareTo(FILE_EXT_EML) == 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static String createTempFolder(String parent) {
        if (Strings.isNullOrEmpty(parent)) {
            parent = System.getProperty("java.io.tmpdir");
        }
        String path = String.format("%s/%s", parent, UUID.randomUUID().toString());
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir.getAbsolutePath();
    }

    public static String cleanDirName(@Nonnull String name) {
        if (!Strings.isNullOrEmpty(name)) {
            name = name.replaceAll("[^A-Za-z0-9@.\\-_]", "_");
            name = name.replaceAll("\\.", "-").toUpperCase();
        }
        return name;
    }

    public static String getFileChecksum(File file) throws IOException {
        try {
            MessageDigest md5Digest = MessageDigest.getInstance("MD5");
            return getFileChecksum(md5Digest, file);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }

    public static String getFileChecksum(MessageDigest digest, File file) throws IOException {
        //Get file input stream for reading the file content
        try (FileInputStream fis = new FileInputStream(file)) {

            //Create byte array to read data in chunks
            byte[] byteArray = new byte[1024];
            int bytesCount = 0;

            //Read file data and update in message digest
            while ((bytesCount = fis.read(byteArray)) != -1) {
                digest.update(byteArray, 0, bytesCount);
            }
        }

        //Get the hash's bytes
        byte[] bytes = digest.digest();

        //This bytes[] has bytes in decimal format;
        //Convert it to hexadecimal format
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
        }

        //return complete hash
        return sb.toString();
    }

    public static List<File> getFiles(@Nonnull File directory) throws IOException {
        Preconditions.checkArgument(directory.exists() && directory.isDirectory());
        List<File> files = new ArrayList<>();
        File[] fs = directory.listFiles();
        if (fs != null && fs.length > 0) {
            for (File f : fs) {
                if (f.isDirectory()) {
                    List<File> dfs = getFiles(f);
                    if (!dfs.isEmpty()) {
                        files.addAll(dfs);
                    } else {
                        files.add(f);
                    }
                }
            }
        }
        return files;
    }

    public static List<File> getFiles(@Nonnull String regex, @Nonnull File directory) throws IOException {
        Preconditions.checkArgument(directory.exists() && directory.isDirectory());
        Preconditions.checkArgument(!Strings.isNullOrEmpty(regex));

        List<File> files = new ArrayList<>();
        FileFilter fileFilter = new RegexFileFilter(regex);

        File[] fs = directory.listFiles(fileFilter);
        if (fs != null && fs.length > 0) {
            for (File f : fs) {
                if (f.isDirectory()) {
                    List<File> dfs = getFiles(regex, f);
                    if (!dfs.isEmpty()) {
                        files.addAll(dfs);
                    } else {
                        files.add(f);
                    }
                }
            }
        }
        return files;
    }

    public static String readTextContent(File file) throws IOException {
        Preconditions.checkArgument(file.exists());
        try (FileInputStream fis = new FileInputStream(file)) {
            StringBuilder builder = new StringBuilder();
            int bufferSize = 4096;
            byte[] buffer = new byte[bufferSize];
            while (true) {
                int size = fis.read(buffer);
                if (size > 0) {
                    String data = new String(buffer, 0, size, StandardCharsets.UTF_8);
                    builder.append(data);
                }
                if (size < bufferSize) break;
            }
            if (builder.length() > 0)
                return builder.toString();
        }
        return null;
    }

    public static String getUniqueFilePath(@Nonnull String filename, String prefix) throws IOException {
        String dp = DateTimeUtils.formatTimestamp("yyyy/MM/dd/HH");
        if (!Strings.isNullOrEmpty(prefix)) {
            dp = String.format("%s/%s", prefix, dp);
        }
        String dir = PathUtils.getTempDir(dp).getAbsolutePath();
        int count = 1;
        String d = dir;
        while (true) {
            String path = String.format("%s/%s", d, filename);
            File f = new File(path);
            if (f.exists()) {
                d = String.format("%s/%d", dir, count);
                count++;
            } else {
                if (!f.mkdirs()) {
                    throw new IOException(String.format("Failed to create directory. [path=%s]", f.getAbsolutePath()));
                }
                return f.getAbsolutePath();
            }
        }
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
        try (FileInputStream fis = new FileInputStream(source)) {
            sourceChannel = fis.getChannel();
            try (FileOutputStream fos = new FileOutputStream(dest)) {
                destChannel = fos.getChannel();
                destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
            }
        } finally {
            if (sourceChannel != null)
                sourceChannel.close();
            if (destChannel != null)
                destChannel.close();
        }
    }

    public static String readContent(@NonNull File file) throws IOException {
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

    public enum EFileTypes {
        Unknown, Text, Archive, Email, Xml, Html, PDF, JSON, MSWord, MSExcel, MSPowerPoint;

        public static String getExtension(EFileTypes type) {
            switch (type) {
                case Xml:
                case Html:
                case PDF:
                case JSON:
                    return type.name().toLowerCase();
                case Text:
                    return "txt";
                case Email:
                    return "elm";
                case Archive:
                    return "zip";
                case MSWord:
                    return "doc";
                case MSExcel:
                    return "xls";
                case MSPowerPoint:
                    return "ppt";
                default:
                    return null;
            }
        }

        public static EFileTypes parse(String mimeType) {
            if (mimeType.indexOf(";") > 0) {
                mimeType = mimeType.split(";")[0];
            }
            if (!Strings.isNullOrEmpty(mimeType)) {
                if (FileUtils.MIME_TYPE_HTML.compareToIgnoreCase(mimeType) == 0) {
                    return Html;
                }
                if (FileUtils.MIME_TYPE_PDF.compareToIgnoreCase(mimeType) == 0) {
                    return PDF;
                }
                if (FileUtils.MIME_TYPE_JSON.compareToIgnoreCase(mimeType) == 0) {
                    return JSON;
                }
                if (FileUtils.MIME_TYPE_TEXT.compareToIgnoreCase(mimeType) == 0) {
                    return Text;
                }
                for (String t : FileUtils.EMAIL_MIME_TYPES) {
                    if (t.compareToIgnoreCase(mimeType) == 0) {
                        return Email;
                    }
                }
                for (String t : FileUtils.XML_MIME_TYPES) {
                    if (t.compareToIgnoreCase(mimeType) == 0) {
                        return Xml;
                    }
                }
                for (String t : FileUtils.ARCHIVE_TYPES) {
                    if (t.compareToIgnoreCase(mimeType) == 0) {
                        return Archive;
                    }
                }
                for (String t : FileUtils.MIME_TYPE_MS_EXCEL) {
                    if (t.compareToIgnoreCase(mimeType) == 0) {
                        return MSExcel;
                    }
                }
                for (String t : FileUtils.MIME_TYPE_MS_POWER_POINT) {
                    if (t.compareToIgnoreCase(mimeType) == 0) {
                        return MSPowerPoint;
                    }
                }
                for (String t : FileUtils.MIME_TYPE_MS_WORD) {
                    if (t.compareToIgnoreCase(mimeType) == 0) {
                        return MSWord;
                    }
                }
            }
            return Unknown;
        }
    }

    public static class FileUtilsException extends Exception {
        private static final String __PREFIX = "Mail Processing Error : %s";

        /**
         * Constructs a new exception with the specified detail message.  The
         * cause is not initialized, and may subsequently be initialized by
         * a call to {@link #initCause}.
         *
         * @param message the detail message. The detail message is saved for
         *                later retrieval by the {@link #getMessage()} method.
         */
        public FileUtilsException(String message) {
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
        public FileUtilsException(String message, Throwable cause) {
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
        public FileUtilsException(Throwable cause) {
            super(String.format(__PREFIX, cause.getLocalizedMessage()), cause);
        }
    }
}
