package ai.sapper.cdc.intake.utils;

import ai.sapper.cdc.common.GlobalConstants;
import ai.sapper.cdc.common.utils.ChecksumUtils;
import ai.sapper.cdc.common.utils.CypherUtils;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.sources.email.ExchangeDataStore;
import ai.sapper.cdc.core.stores.DataStoreException;
import ai.sapper.cdc.core.utils.FileUtils;
import ai.sapper.cdc.intake.flow.TaskContext;
import ai.sapper.cdc.intake.model.*;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import jakarta.mail.*;
import jakarta.mail.internet.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import microsoft.exchange.webservices.data.core.enumeration.property.BodyType;
import microsoft.exchange.webservices.data.core.service.item.EmailMessage;
import microsoft.exchange.webservices.data.core.service.item.Item;
import microsoft.exchange.webservices.data.core.service.schema.EmailMessageSchema;
import microsoft.exchange.webservices.data.property.complex.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedActionException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MailUtils {
    public static final String PREFIX_ATTACHMENT = "__attachment_%s";

    public static class MailProcessingException extends Exception {
        private static final String __PREFIX = "Mail Processing Error : %s";

        /**
         * Constructs a new exception with the specified detail message. The cause is
         * not initialized, and may subsequently be initialized by a call to
         * {@link #initCause}.
         *
         * @param message the detail message. The detail message is saved for later
         *                retrieval by the {@link #getMessage()} method.
         */
        public MailProcessingException(String message) {
            super(String.format(__PREFIX, message));
        }

        /**
         * Constructs a new exception with the specified detail message and cause.
         * <p>
         * Note that the detail message associated with {@code cause} is <i>not</i>
         * automatically incorporated in this exception's detail message.
         *
         * @param message the detail message (which is saved for later retrieval by the
         *                {@link #getMessage()} method).
         * @param cause   the cause (which is saved for later retrieval by the
         *                {@link #getCause()} method). (A <tt>null</tt> value is
         *                permitted, and indicates that the cause is nonexistent or
         *                unknown.)
         * @since 1.4
         */
        public MailProcessingException(String message, Throwable cause) {
            super(String.format(__PREFIX, message), cause);
        }

        /**
         * Constructs a new exception with the specified cause and a detail message of
         * <tt>(cause==null ? null : cause.toString())</tt> (which typically contains
         * the class and detail message of <tt>cause</tt>). This constructor is useful
         * for exceptions that are little more than wrappers for other throwables (for
         * example, {@link PrivilegedActionException}).
         *
         * @param cause the cause (which is saved for later retrieval by the
         *              {@link #getCause()} method). (A <tt>null</tt> value is
         *              permitted, and indicates that the cause is nonexistent or
         *              unknown.)
         * @since 1.4
         */
        public MailProcessingException(Throwable cause) {
            super(String.format(__PREFIX, cause.getLocalizedMessage()), cause);
        }
    }

    /**
     * Business exception to be thrown when there is any issue related to validating an outbound connection. Throw
     * this in cases where you have On behalf permission issues etc.
     */
    public static class OutboundValidationFailedException extends Exception {
        public OutboundValidationFailedException(String message) {
            super(message);
        }
    }

    @Getter
    @Setter
    public static class MimeContent {
        private String name;
        private String mimeType;
        private byte[] content;
        private int size;
        private EFileRecordType type;
        @JsonIgnore
        private File localFile;
    }

    private static final String MAILBOX_REGEX = "\\{MAILBOX=(.+)}";
    private static final String QUERY_REGEX = "\\{QUERY=(.+)}";

    public static String getMailBox(@NonNull String query) throws ParseException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(query));
        try {
            String[] parts = query.split(":");
            for (String part : parts) {
                Pattern pattern = Pattern.compile(MAILBOX_REGEX);
                Matcher m = pattern.matcher(part);
                if (m != null && m.matches()) {
                    if (m.groupCount() > 0) {
                        return m.group(1);
                    }
                }
            }
            return null;
        } catch (Exception ex) {
            throw new ParseException(ex.getLocalizedMessage());
        }
    }

    public static String getMailQuery(@NonNull String query) throws ParseException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(query));
        try {
            String[] parts = query.split(":");
            for (String part : parts) {
                Pattern pattern = Pattern.compile(QUERY_REGEX);
                Matcher m = pattern.matcher(part);
                if (m != null && m.matches()) {
                    if (m.groupCount() > 0) {
                        return m.group(1);
                    }
                }
                return query;
            }
            return null;
        } catch (Exception ex) {
            throw new ParseException(ex.getLocalizedMessage());
        }
    }

    public static String getQuery(String mailbox, @NonNull String query) {
        if (!Strings.isNullOrEmpty(mailbox)) {
            return String.format("{MAILBOX=%s}:{QUERY=%s}", mailbox, query);
        } else
            return String.format("{QUERY=%s}", query);
    }

    public static MimeContent extractInlineContent(String body) throws Exception {
        String[] parts = body.split(":");
        if (parts.length > 1) {
            String type = parts[0];
            if (!Strings.isNullOrEmpty(type) && type.compareToIgnoreCase("data") == 0
                    && !Strings.isNullOrEmpty(parts[1])) {
                String[] content = parts[1].split(";");
                if (content.length > 1) {
                    MimeContent mc = new MimeContent();
                    mc.mimeType = content[0].trim();
                    if (!Strings.isNullOrEmpty(content[1])) {
                        String[] bd = content[1].split(",");
                        if (bd.length > 1) {
                            String ec = bd[0];
                            if (!Strings.isNullOrEmpty(ec) && ec.compareToIgnoreCase("base64") == 0) {
                                mc.content = Base64.decodeBase64(bd[1].getBytes(StandardCharsets.UTF_8));
                            } else {
                                mc.content = bd[1].getBytes(StandardCharsets.UTF_8);
                            }
                        } else {
                            mc.content = content[1].getBytes(StandardCharsets.UTF_8);
                        }
                    }
                    mc.size = mc.content.length;
                    return mc;
                }
            }
        }
        return null;
    }

    public static String getFileNameWithExtension(String mimeType, String filename) {
        String ext = mimeType.replaceAll("/", ".");
        return String.format("%s_%s", filename, ext);
    }

    public static String saveAsEml(@NonNull MessageWrapper message,
                                   @NonNull String folder,
                                   @NonNull String filename)
            throws MailProcessingException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(folder));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(filename));
        try {
            File outf = new File(String.format("%s/%s", folder, filename));
            if (message.getMessageFile() != null && message.getMessageFile().exists()) {
                if (outf.getAbsolutePath().compareTo(message.getMessageFile().getAbsolutePath()) == 0) {
                    return message.getMessageFile().getAbsolutePath();
                } else {
                    FileUtils.copyFile(message.getMessageFile(), outf);
                    return outf.getAbsolutePath();
                }
            }
            if (outf.exists()) {
                if (!outf.delete()) {
                    throw new MailProcessingException(
                            String.format("Error deleting existing file. [file=%s]", outf.getAbsolutePath()));
                }
            }
            try (FileOutputStream fos = new FileOutputStream(outf)) {
                message.getMessage().writeTo(fos);
            }
            message.setMessageFile(outf);
            return outf.getAbsolutePath();
        } catch (Exception ex) {
            throw new MailProcessingException(ex);
        }
    }

    public static File saveToFile(@NonNull AbstractMailMessage<?> message, @NonNull String outputDir)
            throws Exception {
        try {
            if (message instanceof MessageWrapper) {
                String path = saveAsEml((MessageWrapper) message, outputDir);
                if (!Strings.isNullOrEmpty(path)) {
                    File f = new File(path);
                    if (f.exists()) {
                        return f;
                    }
                }
            } else if (message instanceof EmailMessageWrapper) {
                return saveAsEml((EmailMessageWrapper) message, outputDir);
            }
            return null;
        } catch (MailProcessingException e) {
            throw new IOException(e);
        }
    }

    public static String saveAsEml(@NonNull MessageWrapper message, @NonNull String folder)
            throws Exception {
        Preconditions.checkArgument(message != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(folder));
        String filename = String.format("%s.eml", message.getMessageId());
        filename = URLEncoder.encode(CypherUtils.getKeyHash(filename), StandardCharsets.UTF_8);
        return saveAsEml(message, folder, filename);
    }

    public static File saveAsEml(@NonNull EmailMessageWrapper message, @NonNull String outputDir)
            throws IOException {
        Preconditions.checkArgument(message.getMessage() != null);
        try {
            File dir = new File(outputDir);
            if (!dir.exists()) {
                throw new IOException(
                        String.format("saveAsEml() Directory not found. [path=%s]", dir.getAbsolutePath()));
            }
            String fname = String.format("%s.eml",
                    URLEncoder.encode(CypherUtils.getKeyHash(message.getMessageId().getKey()),
                            StandardCharsets.UTF_8));
            fname = fname.replaceAll("[<>]", "");
            File outf = new File(String.format("%s/%s", dir.getAbsolutePath(), fname));
            if (message.getMessageFile() != null && message.getMessageFile().exists()) {
                if (outf.getAbsolutePath().compareTo(message.getMessageFile().getAbsolutePath()) == 0) {
                    return message.getMessageFile();
                } else {
                    FileUtils.copyFile(message.getMessageFile(), outf);
                    return outf;
                }
            }
            try (FileOutputStream fos = new FileOutputStream(outf)) {
                fos.write(message.getMessage().getMimeContent().getContent());
            }
            message.setMessageFile(outf);
            return outf;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public static File saveAsEml(@NonNull EmailMessageWrapper message, @NonNull String outputDir, String filename)
            throws IOException {
        Preconditions.checkArgument(message.getMessage() != null);
        try {
            File dir = new File(outputDir);
            if (!dir.exists()) {
                throw new IOException(
                        String.format("saveAsEml() Directory not found. [path=%s]", dir.getAbsolutePath()));
            }
            filename = URLEncoder.encode(CypherUtils.getKeyHash(
                    FilenameUtils.getBaseName(replaceAllSpecialCharacters(filename))), StandardCharsets.UTF_8);
            String fileExtension = FilenameUtils.getExtension(filename);
            String fname = (fileExtension.equalsIgnoreCase("eml")) ? filename : String.format("%s.eml", filename);
            File outf = new File(getAttachmentFilename(dir.getAbsolutePath(), fname));
            if (message.getMessageFile() != null && message.getMessageFile().exists()) {
                if (outf.getAbsolutePath().compareTo(message.getMessageFile().getAbsolutePath()) == 0) {
                    return message.getMessageFile();
                } else {
                    FileUtils.copyFile(message.getMessageFile(), outf);
                    return outf;
                }
            }
            try (FileOutputStream fos = new FileOutputStream(outf)) {
                fos.write(message.getMessage().getMimeContent().getContent());
            }
            message.setMessageFile(outf);
            return outf;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public static void extractAttachments(@NonNull FileItemRecord parent,
                                          @NonNull EmailMessageWrapper message,
                                          @NonNull String destdir,
                                          @NonNull Session mailSession,
                                          @NonNull String username,
                                          boolean extractText)
            throws MailProcessingException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(destdir));
        String DETACHED_SIGNATURE_MIME_TYPE = "application/pkcs7-signature";
        String multipartSignedExtension = "p7m";

        File dir = new File(String.format("%s/%s/", destdir, message.messageDir()));
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new MailProcessingException(
                        String.format("Error creating destination directory. " + "[path=%s]", dir.getAbsolutePath()));
            }
        }
        try {
            EmailMessage m = message.getMessage();
            if (extractText) {
                File bf = writeBodyToFile(message, dir.getAbsolutePath());
                if (bf != null) {
                    FileItemRecord fr = FileItemRecord.create(parent.getChannel(), bf, "DUMMY_BUCKET", username,
                            parent.getParentId());
                    fr.setRecordType(EFileRecordType.EMAIL_BODY);
                    parent.addFileItemRecord(fr);
                }
            }
            if (!m.getAttachments().getItems().isEmpty()) {
                int count = 0;
                boolean isInlineAttachment = false;
                List<Attachment> attachments = m.getAttachments().getItems();
                for (Attachment attachment : attachments) {
                    isInlineAttachment = attachment.getIsInline();
                    DefaultLogger.debug(String.format("##### attachment name [=%s] [%s]",
                            attachment.getName(),
                            attachment.getContentType()));
                    String attachmentExtension = FilenameUtils.getExtension(attachment.getName());
                    if (multipartSignedExtension.equals(attachmentExtension)) {
                        DefaultLogger.debug("##### Entered p7m block");
                        FileAttachment mimeAttachment = (FileAttachment) attachment;
                        mimeAttachment.load();

                        MimeMessage mimeMessage = createMessageFromBytes(mimeAttachment.getContent());
                        List<Part> extractedSignedAttachments = getAttachmentParts(mimeMessage);
                        for (Part extractedSignedAttachment : extractedSignedAttachments) {
                            DefaultLogger.debug(String.format("####### extractedSignedAttachment name [%s] [%s] [%s]", extractedSignedAttachment.getFileName(),
                                    extractedSignedAttachment.getContentType(), DETACHED_SIGNATURE_MIME_TYPE));
                            if (!DETACHED_SIGNATURE_MIME_TYPE.equals(extractedSignedAttachment.getContentType().split(";")[0])) {
                                // no referenced attachments, as they are all contained in the *.p7m attachment
                                DefaultLogger.debug(String.format("##### extractedSignedAttachment name [=%s] %s", extractedSignedAttachment.getFileName(),
                                        extractedSignedAttachment.getContentType().split(";")[0]));
                                String fname = extractedSignedAttachment.getFileName();
                                if (Strings.isNullOrEmpty(fname)) {
                                    fname = String.format("EmailAttachment-(%d)", count++);
                                }
                                File file = new File(getAttachmentFilename(dir.getAbsolutePath(), fname));
                                if (file.exists()) {
                                    file.delete();
                                }
                                org.apache.commons.io.FileUtils.copyInputStreamToFile(extractedSignedAttachment.getInputStream(), file);

                                FileItemRecord fr = FileItemRecord.create(parent.getChannel(), file, "DUMMY_BUCKET", username,
                                        parent.getParentId());
                                fr.setRecordType(EFileRecordType.EMAIL_ATTACHMENT);
                                fr.setInlineAttachment(isInlineAttachment);
                                parent.addFileItemRecord(fr);
                                if (FileUtils.isEmailFile(file.getAbsolutePath())) {
                                    MessageWrapper km = readFromFile(username, file.getAbsolutePath(), mailSession);
                                    if (km == null) {
                                        throw new Exception(String.format("Error reading email attached. " + "[path=%s]",
                                                file.getAbsolutePath()));
                                    }

                                    extractAttachments(fr, km, dir.getAbsolutePath(), mailSession, username, extractText);
                                    fr.setRecordType(EFileRecordType.EMAIL);

                                } else if (FileUtils.isArchiveFile(file.getAbsolutePath())) {
                                    extractArchive(mailSession, username, fr, extractText);
                                    fr.setRecordType(EFileRecordType.ZIP);
                                }
                            }
                        }

                    } else if (attachment instanceof FileAttachment || isInlineAttachment) {
                        FileAttachment fa = (FileAttachment) attachment;

                        String fname = fa.getName();
                        if (Strings.isNullOrEmpty(fname)) {
                            fname = String.format("EmailAttachment-(%d)", count++);
                        }
                        File file = new File(getAttachmentFilename(dir.getAbsolutePath(), fname));
                        if (file.exists()) {
                            file.delete();
                        }
                        fa.load(file.getAbsolutePath());

                        FileItemRecord fr = FileItemRecord.create(parent.getChannel(), file, "DUMMY_BUCKET", username,
                                parent.getParentId());
                        fr.setRecordType(EFileRecordType.EMAIL_ATTACHMENT);
                        fr.setInlineAttachment(isInlineAttachment);
                        parent.addFileItemRecord(fr);
                        if (FileUtils.isEmailFile(file.getAbsolutePath())) {
                            MessageWrapper km = readFromFile(username, file.getAbsolutePath(), mailSession);
                            if (km == null) {
                                throw new Exception(String.format("Error reading email attached. " + "[path=%s]",
                                        file.getAbsolutePath()));
                            }

                            extractAttachments(fr, km, dir.getAbsolutePath(), mailSession, username, extractText);
                            fr.setRecordType(EFileRecordType.EMAIL);

                        } else if (FileUtils.isArchiveFile(file.getAbsolutePath())) {
                            extractArchive(mailSession, username, fr, extractText);
                            fr.setRecordType(EFileRecordType.ZIP);
                        }
                    } else {
                        ItemAttachment ia = (ItemAttachment) attachment;
                        ia.load(ExchangeDataStore.getPropertySet());
                        Item item = ia.getItem();
                        if (item instanceof EmailMessage) {
                            EmailMessage emailMessage = (EmailMessage) item;
                            DefaultLogger.debug(String.format("##### Mail getContentType. [=%s]", ia.getContentType()));
                            EmailMessageWrapper ew = new EmailMessageWrapper(username, emailMessage, true);
                            File ef = saveAsEml(ew, dir.getAbsolutePath(), ia.getName());
                            FileItemRecord fr = FileItemRecord.create(parent.getChannel(), ef, "DUMMY_BUCKET", username,
                                    parent.getParentId());
                            parent.addFileItemRecord(fr);
                            extractAttachments(fr, ew, dir.getAbsolutePath(), mailSession, username, extractText);
                            fr.setRecordType(EFileRecordType.EMAIL);
                        }
                    }
                }
                DefaultLogger.debug(String.format("##### Mail getAttachments. [=%d]", count));
            }
        } catch (Exception ex) {
            throw new MailProcessingException(ex);
        }
    }

    public static File writeBodyToFile(@NonNull EmailMessageWrapper message, @NonNull String outputDir)
            throws DataStoreException {
        Preconditions.checkArgument(message.getMessage() != null);
        try {
            String body = message.getMessage().getBody().toString();
            if (Strings.isNullOrEmpty(body)) {
                return null;
            }
            File dir = new File(outputDir);
            if (!dir.exists()) {
                throw new DataStoreException(String.format(" writeBodyToFile Directory not found. [path=%s]", dir.getAbsolutePath()));
            }
            String ext = "txt";
            if (message.getMessage().getBody().getBodyType() == BodyType.HTML) {
                ext = "html";
            }
            String fname = String.format("%s.%s", message.getMessageId().getKey(), ext);
            File outf = new File(String.format("%s/%s", dir.getAbsolutePath(), fname));
            try (FileOutputStream fos = new FileOutputStream(outf)) {
                fos.write(body.getBytes(StandardCharsets.UTF_8));
            }
            return outf;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public static File extractEmailBody(@NonNull String mailUser, @NonNull File file, String outfile)
            throws MailProcessingException {
        try {
            if (!FileUtils.isEmailFile(file.getAbsolutePath())) {
                throw new MailProcessingException(
                        String.format("Specified file is not an email file. [path=%s]", file.getAbsolutePath()));
            }
            Session mailSession = Session.getDefaultInstance(new Properties());
            MessageWrapper km = readFromFile(mailUser, file.getAbsolutePath(), mailSession);
            if (km == null) {
                throw new Exception(
                        String.format("Error reading email attached. " + "[path=%s]", file.getAbsolutePath()));
            }
            return getTextFromMessage(km.getMessage(), outfile);
        } catch (Exception ex) {
            throw new MailProcessingException(ex);
        }
    }

    public static String extractEmailBody(@NonNull String mailUser, @NonNull File file) throws MailProcessingException {
        try {
            if (!FileUtils.isEmailFile(file.getAbsolutePath())) {
                throw new MailProcessingException(
                        String.format("Specified file is not an email file. [path=%s]", file.getAbsolutePath()));
            }
            Session mailSession = Session.getDefaultInstance(new Properties());
            MessageWrapper km = readFromFile(mailUser, file.getAbsolutePath(), mailSession);
            if (km == null) {
                throw new Exception(
                        String.format("Error reading email attached. " + "[path=%s]", file.getAbsolutePath()));
            }
            return getTextFromMessage(km.getMessage());
        } catch (Exception ex) {
            throw new MailProcessingException(ex);
        }
    }

    private static File getTextFromMessage(Message message, String outfile) throws MessagingException, IOException {
        String result = "";
        if (message.isMimeType("text/plain")) {
            result = message.getContent().toString();
            if (!Strings.isNullOrEmpty(result)) {
                File fi = new File(String.format("%s.txt", outfile));
                try (FileOutputStream fos = new FileOutputStream(fi)) {
                    fos.write(result.getBytes(StandardCharsets.UTF_8));
                }
                return fi;
            }
        } else if (message.isMimeType("multipart/*")) {
            MimeMultipart mimeMultipart = (MimeMultipart) message.getContent();
            return getTextFromMimeMultipart(mimeMultipart, outfile);
        }
        return null;
    }

    private static String getTextFromMessage(Message message) throws MessagingException, IOException {
        String result = "";
        if (message.isMimeType("text/plain")) {
            result = message.getContent().toString();
        } else if (message.isMimeType("multipart/*")) {
            MimeMultipart mimeMultipart = (MimeMultipart) message.getContent();
            result = getTextFromMimeMultipart(mimeMultipart);
        }
        return result;
    }

    private static String getTextFromMimeMultipart(MimeMultipart mimeMultipart) throws MessagingException, IOException {
        StringBuilder result = new StringBuilder();
        int count = mimeMultipart.getCount();
        for (int i = 0; i < count; i++) {
            BodyPart bodyPart = mimeMultipart.getBodyPart(i);
            if (bodyPart.isMimeType("text/plain")) {
                result.append("\n").append(bodyPart.getContent());
                break; // without break same text appears twice in my tests
            } else if (bodyPart.isMimeType("text/html")) {
                String html = (String) bodyPart.getContent();
                result.append("\n").append(Jsoup.parse(html).text());
            } else if (bodyPart.getContent() instanceof MimeMultipart) {
                result.append(getTextFromMimeMultipart((MimeMultipart) bodyPart.getContent()));
            }
        }
        return result.toString();
    }

    private static File getTextFromMimeMultipart(MimeMultipart mimeMultipart, String outfile)
            throws MessagingException, IOException {
        StringBuilder result = new StringBuilder();
        int count = mimeMultipart.getCount();
        String ext = null;
        for (int i = 0; i < count; i++) {
            BodyPart bodyPart = mimeMultipart.getBodyPart(i);
            if (bodyPart.isMimeType("text/plain")) {
                result.append("\n").append(bodyPart.getContent());
                ext = "txt";
                break; // without break same text appears twice in my tests
            } else if (bodyPart.isMimeType("text/html")) {
                String html = (String) bodyPart.getContent();
                result.append("\n").append(Jsoup.parse(html).text());
                ext = "html";
            } else if (bodyPart.getContent() instanceof MimeMultipart) {
                result.append(getTextFromMimeMultipart((MimeMultipart) bodyPart.getContent()));
            }
        }
        if (ext != null && !Strings.isNullOrEmpty(result.toString())) {
            File fi = new File(String.format("%s.%s", outfile, ext));
            try (FileOutputStream fos = new FileOutputStream(fi)) {
                fos.write(result.toString().getBytes(StandardCharsets.UTF_8));
                fos.flush();
            }
            return fi;
        }
        return null;
    }

    public static void extractAttachments(@NonNull FileItemRecord parent, @NonNull MessageWrapper message,
                                          @NonNull String destdir, @NonNull Session mailSession, @NonNull String username, boolean extractText)
            throws MailProcessingException {
        Preconditions.checkArgument(message != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(destdir));

        File dir = new File(String.format("%s/%s/", destdir, message.messageDir()));
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new MailProcessingException(
                        String.format("Error creating destination directory. " + "[path=%s]", dir.getAbsolutePath()));
            }
        }
        try {
            Message m = message.getMessage();
            Folder folder = m.getFolder();
            if (folder != null && !folder.isOpen())
                folder.open(Folder.READ_WRITE);
            Object content = m.getContent();
            int findex = 0;
            if ((content instanceof Multipart)) {
                Multipart parts = (Multipart) m.getContent();
                for (int i = 0; i < parts.getCount(); i++) {
                    BodyPart bodyPart = parts.getBodyPart(i);

                    String fname = bodyPart.getFileName();

                    if (Strings.isNullOrEmpty(fname)) {
                        fname = String.format(PREFIX_ATTACHMENT, findex++);
                    }

                    if (!Part.ATTACHMENT.equalsIgnoreCase(bodyPart.getDisposition())
                            && Strings.isNullOrEmpty(bodyPart.getFileName())) {
                        if (bodyPart.getContent() == null)
                            continue; // dealing with attachments only
                        else {
                            String mimeType = bodyPart.getContentType();
                            DefaultLogger.debug(String.format("[name=%s][mime type=%s]", fname, mimeType));
                            if (extractText && bodyPart.isMimeType("text/plain")) {
                                fname = String.format("%s.txt", fname);
                                writeTextContent(parent, dir, fname, username, bodyPart);
                            } else if (extractText && bodyPart.isMimeType("text/html")) {
                                fname = String.format("%s.html", fname);
                                writeTextContent(parent, dir, fname, username, bodyPart);
                            } else if (bodyPart.isMimeType("multipart/alternative")
                                    || bodyPart.isMimeType("multipart/mixed")
                                    || bodyPart.isMimeType("multipart/related")) {
                                fname = String.format("%s.mail", fname);
                                writeContent(parent, dir, fname, username, bodyPart, extractText);
                            }
                            continue;
                        }
                    }
                    fname = MimeUtility.decodeText(fname);
                    fname = fname.replaceAll("/", "_");
                    InputStream is = bodyPart.getInputStream();

                    File file = new File(getAttachmentFilename(dir.getAbsolutePath(), fname));

                    try (FileOutputStream fos = new FileOutputStream(file)) {
                        byte[] buf = new byte[4096];
                        int bytesRead;
                        while ((bytesRead = is.read(buf)) != -1) {
                            fos.write(buf, 0, bytesRead);
                        }
                    }
                    Object innerContent = bodyPart.getContent();

                    FileItemRecord fr = FileItemRecord.create(parent.getChannel(), file, "DUMMY_BUCKET", username,
                            parent.getParentId());
                    String disposition = bodyPart.getDisposition();
                    if (disposition != null && disposition.equalsIgnoreCase("inline")) {
                        fr.setInlineAttachment(true);
                    }
                    parent.addFileItemRecord(fr);
                    if (FileUtils.isEmailFile(file.getAbsolutePath()) || innerContent instanceof MimeMessage) {
                        MessageWrapper km = readFromFile(username, file.getAbsolutePath(), mailSession);
                        if (km == null) {
                            throw new Exception(String.format("Error reading email attached. " + "[path=%s]",
                                    file.getAbsolutePath()));
                        }
                        fr.setRecordType(EFileRecordType.EMAIL);
                        extractAttachments(fr, km, dir.getAbsolutePath(), mailSession, username, extractText);

                    } else if (FileUtils.isArchiveFile(file.getAbsolutePath())) {
                        fr.setRecordType(EFileRecordType.ZIP);
                        extractArchive(mailSession, username, fr, extractText);
                    } else {
                        fr.setRecordType(EFileRecordType.EMAIL_ATTACHMENT);
                    }
                }
            }
        } catch (Exception ex) {
            throw new MailProcessingException(ex);
        }
    }

    public static MessageWrapper readFromFile(@NonNull String mailId, @NonNull String filename,
                                              @NonNull Session session) throws IOException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(filename));

        File sf = new File(filename);
        if (!sf.exists())
            throw new IOException(String.format("Mail file not found. [path=%s]", sf.getAbsolutePath()));
        try {
            InputStream fis = new FileInputStream(sf);
            Message message = new MimeMessage(session, fis);
            return new MessageWrapper(mailId, message, true);
        } catch (FileNotFoundException | MessagingException e) {
            throw new IOException(e);
        }
    }

    public static void extractArchive(Session mailSession,
                                      String username,
                                      @NonNull FileItemRecord record,
                                      boolean extractText,
                                      @NonNull String decodingKey,
                                      @NonNull FileSystem fs) throws Exception {
        PathInfo pi = fs.parsePathInfo(record.getFileLocation());
        FileInode inode = (FileInode) fs.getInode(pi);
        if (inode == null) {
            throw new IOException(String.format("File inode not found. [path=%s]", record.getFileLocation().toString()));
        }
        String fname = FilenameUtils.getBaseName(record.getFileName());
        String dir = inode.getParent().getFsPath();
        String outdir = String.format("%s/%s", dir, fname);

        try {
            File odir = new File(outdir);
            if (!odir.exists()) {
                if (!odir.mkdirs()) {
                    throw new MailProcessingException(
                            String.format("Error creating output folder. " + "[path=%s]", odir.getAbsolutePath()));
                }
            }

            if (!record.getPath().exists()) {
                throw new IOException(
                        String.format("Source file not found. [path=%s]", record.getPath().getAbsolutePath()));
            }
            try {
                ZipUtils.unzip(record.getPath().getAbsolutePath(), outdir, decodingKey);
            } catch (Exception ex) {
                record.setState(ERecordState.Error);
                record.setError(ex);
                record.setErrorMessage(ex.getLocalizedMessage());
                return;
            }
            String userId = "Unknown";
            if (!Strings.isNullOrEmpty(username)) {
                userId = username;
            }
            processExtractedFiles(record, mailSession, userId, odir, extractText);
        } catch (IOException | FileUtils.FileUtilsException ex) {
            throw new MailProcessingException(ex);
        } catch (Exception ex) {
            throw ex;
        }
    }

    public static void extractArchive(Session mailSession, String username, @NonNull FileItemRecord file,
                                      boolean extractText) throws Exception {
        String fname = FilenameUtils.getBaseName(file.getFileName());
        String dir = file.getPath().getParent();
        String outdir = String.format("%s/%s", dir, fname);

        try {
            DefaultLogger.debug(String.format("Calling extractArchive [getMessageId=%s]", outdir));
            File odir = new File(outdir);
            if (!odir.exists()) {
                if (!odir.mkdirs()) {
                    throw new MailProcessingException(
                            String.format("Error creating output folder. " + "[path=%s]", odir.getAbsolutePath()));
                }
            }
            if (!file.getPath().exists()) {
                throw new IOException(
                        String.format("Source file not found. [path=%s]", file.getPath().getAbsolutePath()));
            }
            try {
                ZipUtils.unzip(file.getPath().getAbsolutePath(), outdir);
            } catch (Exception ex) {
                file.setState(ERecordState.Error);
                file.setError(ex);
                file.setErrorMessage(ex.getLocalizedMessage());
                return;
            }
            String userId = "Unknown";
            if (!Strings.isNullOrEmpty(username)) {
                userId = username;
            }
            processExtractedFiles(file, mailSession, userId, odir, extractText);
        } catch (IOException | FileUtils.FileUtilsException ex) {
            throw new MailProcessingException(ex);
        } catch (Exception ex) {
            throw ex;
        }
    }

    private static void processExtractedFiles(@NonNull FileItemRecord parent, Session mailSession,
                                              @NonNull String userId, @NonNull File dir, boolean extractText) throws Exception {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    processExtractedFiles(parent, mailSession, userId, f, extractText);
                } else {
                    String parentId = parent.getFileId().getId();
                    FileItemRecord fr = FileItemRecord.create(parent.getChannel(), f, userId, parentId, null);
                    if (mailSession != null && FileUtils.isEmailFile(fr.getPath().getAbsolutePath())) {
                        File ef = fr.getPath();
                        MessageWrapper km = readFromFile(userId, ef.getAbsolutePath(), mailSession);
                        if (km == null) {
                            throw new Exception(String.format("Error reading email attached. " + "[path=%s]",
                                    ef.getAbsolutePath()));
                        }
                        extractAttachments(fr, km, ef.getParent(), mailSession, userId, extractText);
                        fr.setRecordType(EFileRecordType.EMAIL);
                    } else if (FileUtils.isArchiveFile(fr.getPath().getAbsolutePath())) {
                        extractArchive(mailSession, userId, fr, extractText);
                        fr.setRecordType(EFileRecordType.ZIP);
                    } else {
                        fr.setRecordType(EFileRecordType.ZIP_CONTENT);
                    }
                    parent.addFileItemRecord(fr);
                }
            }
        }
    }

    private static String getAttachmentFilename(String dir, String filename) {
        int ii = 0;
        String path = String.format("%s/%s", dir, filename);
        while (true) {
            File fi = new File(path);
            if (!fi.exists()) {
                path = fi.getAbsolutePath();
                DefaultLogger.info(String.format("getAttachmentFilename, new file before altering complete path [%s]", path));
                break;
            }
            String ext = FilenameUtils.getExtension(fi.getName());
            String name = FilenameUtils.getBaseName(fi.getName());
            String fname = String.format("%s_%d.%s", name, ii++, ext);
            path = String.format("%s/%s", dir, fname);
            DefaultLogger.info(String.format("getAttachmentFilename, new file after altering complete path [%s]", path));
        }
        return path;
    }

    private static void writeContent(FileItemRecord parent, File dir, String filename, String username,
                                     BodyPart bodyPart, boolean extractText) throws Exception {
        List<MimeContent> contents = getContent(bodyPart, null);
        if (contents != null && !contents.isEmpty()) {
            int index = 0;
            for (MimeContent content : contents) {
                FileUtils.EFileTypes type = FileUtils.EFileTypes.parse(content.mimeType);
                if (type == FileUtils.EFileTypes.Html || type == FileUtils.EFileTypes.Text) {
                    if (!extractText)
                        continue;
                }
                String ext = FileUtils.EFileTypes.getExtension(type);
                if (Strings.isNullOrEmpty(ext)) {
                    ext = content.mimeType;
                    if (ext.indexOf(";") > 0) {
                        ext = ext.split(";")[0];
                    }
                    ext = ext.replaceAll("/", "\\.");
                }
                String fname = filename;
                if (!Strings.isNullOrEmpty(content.name)) {
                    fname = content.name;
                } else {
                    fname = String.format("%s_%d.%s", fname, index, ext);
                }
                File file = new File(String.format("%s/%s", dir.getAbsolutePath(), fname));
                try (FileOutputStream fos = new FileOutputStream(file)) {
                    fos.write(content.content);
                    fos.flush();
                }
                FileItemRecord fr = FileItemRecord.create(parent.getChannel(), file, "DUMMY_BUCKET", username,
                        parent.getParentId());
                if (type == FileUtils.EFileTypes.Email) {
                    fr.setRecordType(EFileRecordType.EMAIL);
                } else {
                    fr.setRecordType(content.type);
                }
                parent.addFileItemRecord(fr);
            }
        }
    }

    private static void writeTextContent(FileItemRecord parent, File dir, String filename, String username,
                                         BodyPart bodyPart) throws Exception {
        File file = new File(String.format("%s/%s", dir.getAbsolutePath(), filename));
        String content = getText(bodyPart);
        if (!Strings.isNullOrEmpty(content)) {
            try (FileOutputStream fos = new FileOutputStream(file)) {
                fos.write(content.getBytes());
            }
            String type = FileUtils.detectMimeType(file);
            String next = null;
            if (!Strings.isNullOrEmpty(type)) {
                if (type.toLowerCase().startsWith("text/")) {
                    String[] parts = type.split("/");
                    next = parts[1];
                } else if (type.toLowerCase().startsWith("image/")) {
                    String[] parts = type.split("/");
                    next = parts[1];
                }
                if (!Strings.isNullOrEmpty(next)) {
                    String path = file.getAbsolutePath();
                    path = String.format("%s.%s", path, next);
                    File nfile = new File(path);
                    DefaultLogger.debug(String.format("Renaming file. [from=%s][to=%s]",
                            file.getAbsolutePath(), nfile.getAbsolutePath()));

                    if (!file.renameTo(nfile)) {
                        throw new Exception(String.format("Error renaming file. [from=%s][to=%s]",
                                file.getAbsolutePath(), nfile.getAbsolutePath()));
                    }
                    file = nfile;
                }
                FileItemRecord fr = FileItemRecord.create(parent.getChannel(), file, "DUMMY_BUCKET", username,
                        parent.getParentId());
                fr.setRecordType(EFileRecordType.EMAIL_BODY);
                parent.addFileItemRecord(fr);
            }
        }
    }

    private static List<MimeContent> addContent(String name, EFileRecordType type, String mimeType, byte[] data,
                                                List<MimeContent> contents) {
        if (data != null) {
            if (contents == null)
                contents = new ArrayList<>();
            MimeContent content = new MimeContent();
            content.name = name;
            content.mimeType = mimeType;
            content.content = data;
            content.size = content.content.length;
            content.type = type;
            contents.add(content);
        }
        return contents;
    }

    private static List<MimeContent> getContent(Part part, List<MimeContent> contents) throws Exception {
        DefaultLogger.debug(String.format("Parent mime-type [%s]", part.getContentType()));
        if (part.isMimeType("text/*")) {
            String text = (String) part.getContent();
            if (text != null && !Strings.isNullOrEmpty(text.trim())) {
                EFileRecordType type = EFileRecordType.EMAIL_ATTACHMENT;
                if (!Part.ATTACHMENT.equalsIgnoreCase(part.getDisposition())) {
                    type = EFileRecordType.EMAIL_BODY;
                }
                contents = addContent(part.getFileName(), type, part.getContentType(),
                        text.getBytes(StandardCharsets.UTF_8), contents);
            }
            return contents;
        }

        if (part.isMimeType("multipart/alternative") || part.isMimeType("multipart/related")) {
            // prefer html text over plain text
            Multipart mp = (Multipart) part.getContent();
            for (int i = 0; i < mp.getCount(); i++) {
                Part bp = mp.getBodyPart(i);
                DefaultLogger.debug(String.format("Parent mime-type [%s]", bp.getContentType()));
                if (bp.isMimeType("multipart/alternative") || bp.isMimeType("multipart/related")) {
                    contents = getContent(bp, contents);
                } else if (bp.isMimeType("text/plain") || bp.isMimeType("text/html")) {
                    String text = getText(bp);
                    if (!Strings.isNullOrEmpty(text)) {
                        EFileRecordType type = EFileRecordType.EMAIL_ATTACHMENT;
                        if (!Part.ATTACHMENT.equalsIgnoreCase(bp.getDisposition())) {
                            type = EFileRecordType.EMAIL_BODY;
                        }
                        contents = addContent(bp.getFileName(), type, bp.getContentType(),
                                text.getBytes(StandardCharsets.UTF_8), contents);
                    }
                } else {
                    Object content = bp.getContent();
                    if (content instanceof InputStream) {
                        byte[] data = ByteStreams.toByteArray((InputStream) content);
                        if (data != null && data.length > 0) {
                            contents = addContent(bp.getFileName(), EFileRecordType.EMAIL_ATTACHMENT,
                                    bp.getContentType(), data, contents);
                        }
                    }
                }
            }
        } else if (part.isMimeType("multipart/*")) {
            Multipart mp = (Multipart) part.getContent();
            for (int i = 0; i < mp.getCount(); i++) {
                Part bp = mp.getBodyPart(i);
                contents = getContent(bp, contents);
            }
        }
        return contents;
    }

    private static String getText(Part part) throws Exception {
        if (part.isMimeType("text/*")) {
            return (String) part.getContent();
        }

        if (part.isMimeType("multipart/alternative")) {
            // prefer html text over plain text
            Multipart mp = (Multipart) part.getContent();
            String text = null;
            for (int i = 0; i < mp.getCount(); i++) {
                Part bp = mp.getBodyPart(i);
                if (bp.isMimeType("text/plain")) {
                    if (text == null)
                        text = getText(bp);
                    continue;
                } else if (bp.isMimeType("text/html")) {
                    String s = getText(bp);
                    if (s != null)
                        return s;
                } else {
                    return getText(bp);
                }
            }
            return text;
        } else if (part.isMimeType("multipart/*")) {
            Multipart mp = (Multipart) part.getContent();
            for (int i = 0; i < mp.getCount(); i++) {
                String s = getText(mp.getBodyPart(i));
                if (s != null)
                    return s;
            }
        }
        return null;
    }

    public static MessageHeader parseHeader(@NonNull AbstractMailMessage<?> m) throws Exception {
        MessageHeader header = new MessageHeader();

        if (m instanceof MessageWrapper) {
            MessageWrapper message = (MessageWrapper) m;
            if (message.getMessageId() != null)
                header.setId(message.getMessageId().getKey());
            header.setFrom(convertTo(message.getMessage().getFrom()));
            header.setTo(convertTo(message.getMessage().getRecipients(Message.RecipientType.TO)));
            header.setCc(convertTo(message.getMessage().getRecipients(Message.RecipientType.CC)));
            header.setBcc(convertTo(message.getMessage().getRecipients(Message.RecipientType.BCC)));
            header.setReplyTo(convertTo(message.getMessage().getReplyTo()));
            if (message.getMessage().getReceivedDate() != null)
                header.setReceivedDate(message.getMessage().getReceivedDate().getTime());
            if (message.getMessage().getSentDate() != null)
                header.setSendDate(message.getMessage().getSentDate().getTime());
            header.setSubject(message.getMessage().getSubject());
        } else if (m instanceof EmailMessageWrapper) {
            EmailMessageWrapper message = (EmailMessageWrapper) m;
            if (message.getMessageId() != null)
                header.setId(message.getMessageId().getKey());
            if (message.getMessage().getPropertyBag().contains(EmailMessageSchema.Subject))
                header.setSubject(message.getMessage().getSubject());
            if (message.getMessage().getPropertyBag().contains(EmailMessageSchema.DateTimeSent))
                if (message.getMessage().getDateTimeSent() != null)
                    header.setSendDate(message.getMessage().getDateTimeSent().getTime());
            if (message.getMessage().getPropertyBag().contains(EmailMessageSchema.DateTimeReceived))
                if (message.getMessage().getDateTimeReceived() != null)
                    header.setReceivedDate(message.getMessage().getDateTimeReceived().getTime());
            if (message.getMessage().getPropertyBag().contains(EmailMessageSchema.From))
                header.setFrom(new String[]{message.getMessage().getFrom().getAddress()});
            if (message.getMessage().getPropertyBag().contains(EmailMessageSchema.ToRecipients))
                header.setTo(getAddresses(message.getMessage().getToRecipients()));
            if (message.getMessage().getPropertyBag().contains(EmailMessageSchema.CcRecipients))
                header.setCc(getAddresses(message.getMessage().getCcRecipients()));
            if (message.getMessage().getPropertyBag().contains(EmailMessageSchema.BccRecipients))
                header.setBcc(getAddresses(message.getMessage().getBccRecipients()));
            if (message.getMessage().getPropertyBag().contains(EmailMessageSchema.ReplyTo))
                header.setReplyTo(getAddresses(message.getMessage().getReplyTo()));
        }
        return header;
    }

    private static String getEmailAddress(String address) {
        if (address != null && address.indexOf("<") != -1 && address.indexOf(">") != -1) {
            Matcher matcher = Pattern.compile("<(.*?)>").matcher(address);
            if (matcher.find()) {
                address = matcher.group(1);
            }
        }
        address = (address != null) ? address.replaceAll("[^A-Za-z0-9_@.-]", "") : address;
        return address;
    }

    private static String[] convertTo(Address[] addresses) throws AddressException {
        if (addresses != null && addresses.length > 0) {
            String[] array = new String[addresses.length];
            int ii = 0;
            for (Address address : addresses) {
                String ia = getEmailAddress(address.toString());
                array[ii] = ia;
                ii++;
            }
            return array;
        }
        return null;
    }

    public static String[] getAddresses(EmailAddressCollection addresses) throws AddressException {
        String[] array = new String[addresses.getCount()];
        int ii = 0;
        for (EmailAddress address : addresses) {
            array[ii] = getEmailAddress(address.getAddress());
            ii++;
        }
        return array;
    }

    public static String generateHeaderJson(@NonNull MessageHeader header) throws MailProcessingException {
        try {
            String json = GlobalConstants.getJsonMapper().writeValueAsString(header);
            DefaultLogger.debug(String.format("HEADER=[%s]", json));
            return json;
        } catch (Exception ex) {
            throw new MailProcessingException(ex);
        }
    }

    public static String generateTaskContextJson(@NonNull TaskContext context) throws MailProcessingException {
        return null;
    }

    public static String generateMessageHash(@NonNull MessageHeader header) throws MailProcessingException {
        try {
            String str = header.toString();

            String hash = ChecksumUtils.getKeyHash(str);
            DefaultLogger.debug(String.format("HEADER=[%s]\nKEY=[%s]", str, hash));

            return hash;
        } catch (Exception ex) {
            throw new MailProcessingException(ex);
        }
    }

    /**
     * Utility method for validating input for adding email connections
     *
     * @param emailDataSourceConnectionJson
     */
    public static void validateEmailDataSourceConnectionJson(EmailDataSourceConnectionJson emailDataSourceConnectionJson) throws IllegalArgumentException {
        List<EmailConnectionJson> emailConnections = emailDataSourceConnectionJson.getEmailConnections();
        if (emailConnections == null || emailConnections.isEmpty()) {
            throw new IllegalArgumentException("emailConnections should not be null or empty");
        }

        //check mandatory fields in the input
        for (EmailConnectionJson emailConnectionJson : emailConnections) {

            Preconditions.checkArgument(!Strings.isNullOrEmpty(emailConnectionJson.getEmailType()),
                    "[emailType] field for emailConnections cannot be null or empty");
            EmailType emailType = EmailType.valueOfIgnoreCase(emailConnectionJson.getEmailType());
            //checking emailType for valid enum value
            if (emailType == null) {
                throw new IllegalArgumentException(String.format("Invalid emailType : [emailType=%s]", emailConnectionJson.getEmailType()));
            }

            Preconditions.checkArgument(!Strings.isNullOrEmpty(emailConnectionJson.getEmailId()),
                    "[emailId] field for an emailConnection cannot be null or empty");
            Preconditions.checkArgument(!Strings.isNullOrEmpty(emailConnectionJson.getAccount()),
                    "[account] field for an emailConnection cannot be null or empty");

            //password,serverId and channel are not mandatory for adding outbound connections
            if (!emailType.equals(EmailType.Outbound)) {
                Preconditions.checkArgument(!Strings.isNullOrEmpty(emailConnectionJson.getPassword()),
                        "[password] field for emailConnection cannot be null or empty");
                Preconditions.checkArgument(!Strings.isNullOrEmpty(emailConnectionJson.getChannel()),
                        "[channel] field for emailConnections cannot be null or empty");
                Preconditions.checkArgument(!Strings.isNullOrEmpty(emailConnectionJson.getServerId()),
                        "[serverId] field for an emailConnection cannot be null or empty");

                //checking channel for enum value. Channel should not be mandatory for outbound
                if (EIntakeChannel.valueOfIgnoreCase(emailConnectionJson.getChannel()) == null) {
                    throw new IllegalArgumentException(String.format("Invalid channel : [channel=%s]", emailConnectionJson.getChannel()));
                }

            }


        }
    }

    public static final String CONTENT_TYPE_ID = "Content-Type";
    public static final String CONTENT_ID_ID = "Content-ID";

    public static final String CONTENT_TRANSFER_ENCODING_ID = "Content-Transfer-Encoding";
    public static final String QUOTED_PRINTABLE = "quoted-printable";

    public static final String CONTENT_TYPE_TEXT_HTML = "text/html; charset=\"UTF-8\"";
    public static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain; charset=\"UTF-8\"";
    public static final String CONTENT_TYPE_MESSAGE_RFC822 = "message/rfc822";
    public static final String CONTENT_TYPE_IMAGE_PREFIX = "image/";
    public static final String CONTENT_TYPE_MULTIPART_PREFIX = "multipart/";

    public static final String HEADER_IN_REPLY_TO = "In-Reply-To";

    /**
     * @return {@link MimeMessage} created out of given {@code byte[]}
     */
    public static MimeMessage createMessageFromBytes(byte[] bytes) {
        return createMessageFromBytes(bytes, null);
    }

    /**
     * @return {@link MimeMessage} created out of given {@code byte[]}
     */
    public static MimeMessage createMessageFromBytes(byte[] bytes, Session session) {
        try {
            ByteArrayInputStream st = new ByteArrayInputStream(bytes);
            return new MimeMessage(session, st);
        } catch (Exception e) {
            throw new RuntimeException("Unexpected: ", e);
        }
    }


    /**
     * Returns a list of attachments parts.
     *
     * @param message Message to look for attachment parts.
     */
    public static List<Part> getAttachmentParts(Part message) {
        List<Part> attachmentCollector = new ArrayList<>();
        collectMailParts(message, null, attachmentCollector, null);
        return attachmentCollector;
    }


    /**
     * Collects the body, attachment and inline attachment parts from the provided part.
     * <p>
     * A single collector can be null in order to collect only the relevant parts.
     *
     * @param part                      Part
     * @param bodyCollector             Body collector (optional)
     * @param attachmentCollector       Attachment collector (optional)
     * @param inlineAttachmentCollector Inline attachment collector (optional)
     */
    public static void collectMailParts(Part part, List<Part> bodyCollector, List<Part> attachmentCollector, List<Part> inlineAttachmentCollector) {
        if (part == null) {
            return;
        }
        try {
            String disposition = getDispositionSafely(part);
            if (disposition != null && disposition.equalsIgnoreCase(Part.ATTACHMENT)) {
                if (attachmentCollector != null) {
                    attachmentCollector.add(part);
                }
            } else {
                Object content = null;
                try { // NOSONAR
                    // getContent might throw a MessagingException for legitimate parts (e.g. some images end up in a javax.imageio.IIOException for example).
                    content = getPartContent(part);
                } catch (MessagingException | IOException e) {
                    DefaultLogger.info(String.format("Unable to get mime part content due to {%s}: {%s}", e.getClass().getSimpleName(), e.getMessage()));
                }

                if (content instanceof Multipart) {
                    Multipart multiPart = (Multipart) content;
                    for (int i = 0; i < multiPart.getCount(); i++) {
                        collectMailParts(multiPart.getBodyPart(i), bodyCollector, attachmentCollector, inlineAttachmentCollector);
                    }
                } else {
                    if (part.isMimeType(CONTENT_TYPE_TEXT_PLAIN)) {
                        if (bodyCollector != null) {
                            bodyCollector.add(part);
                        }
                    } else if (part.isMimeType(CONTENT_TYPE_TEXT_HTML)) {
                        if (bodyCollector != null) {
                            bodyCollector.add(part);
                        }
                    } else if (part.isMimeType(CONTENT_TYPE_MESSAGE_RFC822) && content instanceof MimeMessage) {
                        // it's a MIME message in rfc822 format as attachment therefore we have to set the filename for the attachment correctly.
                        if (attachmentCollector != null) {
                            String fileName = getFilenameFromRfc822Attachment(part);
                            if (Strings.isNullOrEmpty(fileName)) {
                                fileName = "originalMessage.eml";
                            }
                            RFCWrapperPart wrapperPart = new RFCWrapperPart(part, fileName);
                            attachmentCollector.add(wrapperPart);
                        }
                    } else if (disposition != null && disposition.equals(Part.INLINE)) {
                        if (inlineAttachmentCollector != null) {
                            inlineAttachmentCollector.add(part);
                        }
                    } else {
                        String[] headerContentId = part.getHeader(CONTENT_ID_ID);
                        if (headerContentId != null && headerContentId.length > 0 && !StringUtils.isBlank(headerContentId[0]) && part.getContentType() != null && part.getContentType().startsWith(CONTENT_TYPE_IMAGE_PREFIX)) {
                            if (inlineAttachmentCollector != null) {
                                inlineAttachmentCollector.add(part);
                            }
                        } else if (part.getFileName() != null /* assumption: file name = attachment (last resort) */) {
                            if (attachmentCollector != null) {
                                attachmentCollector.add(part);
                            }
                        } else {
                            DefaultLogger.debug(String.format("Unknown mail message part, headers: [{%s}]", part.getAllHeaders()));
                        }
                    }
                }
            }
        } catch (MessagingException | IOException e) {
            throw new RuntimeException("Unexpected: ", e);
        }
    }


    public static String getFilenameFromRfc822Attachment(Part part) throws MessagingException, IOException {
        if (part == null) {
            return null;
        }
        Object content = getPartContent(part);
        if (!(content instanceof MimeMessage)) {
            return null;
        }
        String subject = ((MimeMessage) content).getSubject();
        if (!StringUtils.isBlank(subject)) {
            String name = PathUtils.toValidFilename(subject);
            if (!StringUtils.isBlank(name)) {
                return name + "." + guessAttachmentFileExtension(part.getContentType());
            }
        }
        return null;
    }


    protected static String guessAttachmentFileExtension(String contentType) {
        if (contentType == null) {
            return null;
        } else {
            ContentType ct;
            try {
                ct = new ContentType(contentType);
            } catch (ParseException ex) {
                DefaultLogger.warn(String.format("Failed to parse content type '%s'", contentType));
                return null;
            }

            String baseType = ct.getBaseType();
            MimeTypes mimeType = MimeTypes.convertToMimeType(baseType);
            return mimeType == null ? null : mimeType.getFileExtension();
        }
    }

    protected static Object getPartContent(Part part) throws MessagingException, IOException {
        if (part == null) {
            return null;
        }
        if (part instanceof MimePart) {
            autoFixEncoding((MimePart) part);
        }
        autoFixCharset(part);
        return part.getContent();
    }


    /**
     * Fixes a special behavior when access of {@link Part#getContent()} results in an {@link NullPointerException} due to
     * an invalid charset.
     * <p>
     * If charset is valid or {@link Part#getContent()} doesn't result in a {@link NullPointerException}, no fix is
     * applied.
     * <p>
     * As java.io.InputStreamReader.InputStreamReader(InputStream, String) might throw up with a
     * {@link NullPointerException} in case of an unknown character set, if such an exception occurs, the character set is
     * replaced with {@link StandardCharsets#UTF_8} even though this may lead to display errors.
     * </p>
     */
    protected static void autoFixCharset(Part part) {
        String charset = null;
        try {
            charset = getPartCharsetInternal(part);
            if (charset == null || Charset.isSupported(charset)) {
                return;
            }
            part.getContent();
        } catch (NullPointerException | UnsupportedEncodingException e) { // NOSONAR
            String msgId = null;
            if (part instanceof MimeMessage) {
                msgId = getMessageIdSafely((MimeMessage) part);
            }
            DefaultLogger.info(String.format("Mail part '{%s}' uses an unsupported character set '{%s}'. Use UTF-8 as fallback.", msgId, charset));
            // Update charset of content type so that when accessing part.getContent() again no NPE is thrown
            // (UnsupportedEncodingException might still be thrown when part.getContent() didn't result in an NPE).
            if (charset == null) {
                return; // cannot fix
            }
            try {
                String contentType = part.getContentType(); // cannot be null because otherwise charset would have been null already
                part.setHeader(CONTENT_TYPE_ID, contentType.replace(charset, StandardCharsets.UTF_8.name()));
            } catch (Exception ex) {
                DefaultLogger.info(String.format("Unable to switch to default character set for message with ID '{%s}'.", msgId));
            }
        } catch (Exception e) {
            DefaultLogger.trace(e); // explicitly trace
        }
    }


    /**
     * Some parts use an invalid encoding (not to be confused with the charset!) which is not supported. If this is
     * detected, remove it and try with the default encoding.<br>
     * Supported encodings are listed in {@link MimeUtility#decode(InputStream, String)} If this fix is not applied, a
     * MessagingException would be thrown on {@link Part#getContent()}
     */
    protected static void autoFixEncoding(MimePart part) {
        if (part == null) {
            return;
        }
        String encoding = null;
        try {
            encoding = part.getEncoding();
            if (encoding == null) {
                return; // no encoding present. No need to test it
            }
            part.getContent(); // triggers the decode
        } catch (Exception e) {
            // use exception message as indicator because no list of supported encodings exists to be checked against
            String exMsg = e.getMessage();
            String identifier = "Unknown encoding";
            String msgId = null;
            if (part instanceof MimeMessage) {
                msgId = getMessageIdSafely((MimeMessage) part);
            }
            if (exMsg != null && exMsg.regionMatches(true, 0, identifier, 0, identifier.length())) {
                // Supplied encoding is not supported.
                // Remove it as otherwise MimeUtility#decode fails with MessagingException
                // If no header is supplied the data will be read as is (no special decoding)
                DefaultLogger.info(String.format("Mail part '{%s}' uses an unsupported Content-Transfer-Encoding '{%s}' . Use default encoding as fallback.", msgId, encoding));
                try {
                    part.removeHeader("Content-Transfer-Encoding");
                } catch (Exception ex) {
                    DefaultLogger.info(String.format("Unable to switch to default encoding for message with ID '{%s}'.", msgId));
                }
            } else {
                DefaultLogger.trace(String.format("autoFixTransferEncoding: Exception has occurred for message with ID '{%s}'. [error=%s]", msgId, e.getLocalizedMessage())); // explicitly trace
            }
        }
    }

    /**
     * Retrieves the value of the 'Message-Id' header field of the message, without throwing an exception.
     *
     * @param mimeMessage message
     * @return Message-Id or {@code null}
     */
    public static String getMessageIdSafely(MimeMessage mimeMessage) {
        if (mimeMessage == null) {
            return null;
        }

        try {
            return mimeMessage.getMessageID();
        } catch (MessagingException e) {
            DefaultLogger.info(e.getLocalizedMessage());
            return null;
        }
    }


    /**
     * @return Charset as string
     */
    protected static String getPartCharsetInternal(Part part) throws MessagingException {
        if (part == null) {
            return null;
        }

        String contentType = part.getContentType();
        if (contentType == null) {
            return null;
        }

        try {
            return new ContentType(contentType).getParameter("charset");
        } catch (jakarta.mail.internet.ParseException e) {
            DefaultLogger.trace(String.format("Failed to parse content type '{%s}'", contentType));
            return null;
        }
    }

    protected static String getDispositionSafely(Part part) {
        try {
            return part.getDisposition();
        } catch (MessagingException e) {
            DefaultLogger.info("Unable to get disposition");
            // Rare cases where content disposition header is set but empty, assuming no disposition at all.
            return null;
        }
    }

    public static String replaceAllSpecialCharacters(String value) {
        return value.replaceAll("[|?*\\\\/:\"<>.]", "_");
    }
}
