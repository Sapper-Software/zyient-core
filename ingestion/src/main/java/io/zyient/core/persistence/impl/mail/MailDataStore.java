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

package io.zyient.core.persistence.impl.mail;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.utils.CollectionUtils;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.model.StringKey;
import io.zyient.core.persistence.DataStoreException;
import io.zyient.core.persistence.impl.mail.model.AbstractMailMessage;
import io.zyient.core.persistence.impl.mail.model.EmailMessage;
import io.zyient.core.persistence.impl.mail.model.MessageWrapper;
import io.zyient.core.persistence.impl.mail.restq.MailRQLParser;
import io.zyient.core.persistence.impl.mail.settings.MailDataSourceSettings;
import jakarta.activation.DataHandler;
import jakarta.activation.DataSource;
import jakarta.activation.FileDataSource;
import jakarta.mail.*;
import jakarta.mail.internet.*;
import jakarta.mail.search.MessageIDTerm;
import jakarta.mail.search.SearchTerm;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Getter
@Accessors(fluent = true)
public class MailDataStore extends AbstractMailDataStore<Store, Message, Folder> {
    public static final String DEFAULT_IMAP_FOLDER = "INBOX";
    @Setter(AccessLevel.NONE)
    private IMAPConnection imapConnection;
    @Setter(AccessLevel.NONE)
    private SMTPConnection smtpConnection;
    @Getter(AccessLevel.NONE)
    private Map<String, Folder> openedFolders = new HashMap<>();


    @Override
    public void configure() throws ConfigurationException {
        Preconditions.checkArgument(settings instanceof MailDataSourceSettings);
        MailDataSourceSettings settings = (MailDataSourceSettings) settings();
        try {
            Preconditions.checkArgument(connection() instanceof IMAPConnection);
            imapConnection = (IMAPConnection) connection();
            Store store = imapConnection.connection();
            if (!store.isConnected()) {
                throw new ConfigurationException(String.format("Store not connected. [name=%s]", name()));
            }
            String sendConnection = settings.getSmtpConnectionName();
            if (!Strings.isNullOrEmpty(sendConnection)) {
                smtpConnection = dataStoreManager().getConnection(sendConnection, SMTPConnection.class);
                if (smtpConnection == null) {
                    throw new ConfigurationException(String.format("SMTP Connection not found. [type=%s][name=%s]",
                            SMTPConnection.class.getCanonicalName(),
                            sendConnection));
                }
            }
            mailbox = settings.getMailbox();
            if (Strings.isNullOrEmpty(mailbox)) {
                mailbox = DEFAULT_IMAP_FOLDER;
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public MessageWrapper createWrappedMessage(@NonNull String mailId,
                                               @NonNull EmailMessage email,
                                               @NonNull String sender) throws DataStoreException {
        Message message = createMessage(mailId, email, sender);
        return new MessageWrapper(mailId, message);
    }

    @Override
    public String getMailUser() {
        if (imapConnection != null) {
            return imapConnection.getEmailId();
        }
        return null;
    }

    @Override
    public Message createMessage(@NonNull String mailId, @NonNull EmailMessage email, @NonNull String sender)
            throws DataStoreException {
        try {
            Session session = null;
            if (smtpConnection != null) {
                session = smtpConnection.connection();
            } else {
                throw new DataStoreException("SMTP connection not defined...");
            }
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(sender));

            message.setRecipients(Message.RecipientType.TO, parseAddresses(email.getHeader().getTo()));
            if (email.getHeader().getCc() != null && email.getHeader().getCc().length > 0) {
                message.setRecipients(Message.RecipientType.CC, parseAddresses(email.getHeader().getCc()));
            }
            if (email.getHeader().getBcc() != null && email.getHeader().getBcc().length > 0) {
                message.setRecipients(Message.RecipientType.BCC, parseAddresses(email.getHeader().getBcc()));
            }
            if (!Strings.isNullOrEmpty(email.getHeader().getSubject())) {
                message.setSubject(email.getHeader().getSubject());
            }
            message.setHeader(MessageWrapper.PARAM_MESSAGE_ID, UUID.randomUUID().toString());
            if (email.getHeader().getReplyTo() != null) {
                message.setReplyTo(parseAddresses(email.getHeader().getReplyTo()));
            }
            if (!Strings.isNullOrEmpty(email.getBody()) && email.getAttachments() == null) {
                String body = email.getBody();
                message.setText(body, StandardCharsets.UTF_8.name(), "html");
            } else if (!Strings.isNullOrEmpty(email.getBody()) && email.getAttachments() != null) {
                BodyPart messageBodyPart = new MimeBodyPart();
                messageBodyPart.setContent(email.getBody(), "text/html");
                Multipart multipart = new MimeMultipart();
                multipart.addBodyPart(messageBodyPart);
                for (String attachment : email.getAttachments()) {
                    addAttachment(multipart, attachment);
                }
                message.setContent(multipart, "text/html");
            } else if (email.getAttachments() != null) {
                Multipart multipart = new MimeMultipart();
                for (String attachment : email.getAttachments()) {
                    addAttachment(multipart, attachment);
                }
                message.setContent(multipart);
            }

            return message;
        } catch (Exception e) {
            throw new DataStoreException(e);
        }
    }

    private Address[] parseAddresses(String[] addresses) throws AddressException {
        if (addresses != null && addresses.length > 0) {
            Address[] array = new Address[addresses.length];
            for (int ii = 0; ii < addresses.length; ii++) {
                array[ii] = new InternetAddress(addresses[ii]);
            }
            return array;
        }
        return null;
    }

    private void addAttachment(Multipart multipart, String attachment)
            throws MessagingException {
        MimeBodyPart messageBodyPart = new MimeBodyPart();
        DataSource source = new FileDataSource(attachment);
        messageBodyPart.setDataHandler(new DataHandler(source));
        Path p = Paths.get(attachment);
        messageBodyPart.setFileName(p.getFileName().toString());
        multipart.addBodyPart(messageBodyPart);
    }

    @Override
    public MessageWrapper createMessage(@NonNull String mailId, @NonNull String sender, @NonNull String[] sendTo,
                                        String[] ccTo, String[] bccTo, String subject) throws DataStoreException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sender));
        Preconditions.checkArgument(sendTo != null && sendTo.length > 0);
        try {
            Session session = null;
            if (smtpConnection != null) {
                session = smtpConnection.connection();
            } else {
                throw new DataStoreException("SMTP Connection not defined...");
            }
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(sender));
            InternetAddress[] addTo = new InternetAddress[sendTo.length];
            for (int ii = 0; ii < sendTo.length; ii++) {
                InternetAddress ta = new InternetAddress(sendTo[ii]);
                addTo[ii] = ta;
            }
            message.setRecipients(Message.RecipientType.TO, addTo);
            if (ccTo != null && ccTo.length > 0) {
                InternetAddress[] addCC = new InternetAddress[ccTo.length];
                for (int ii = 0; ii < ccTo.length; ii++) {
                    InternetAddress ta = new InternetAddress(ccTo[ii]);
                    addCC[ii] = ta;
                }
                message.setRecipients(Message.RecipientType.CC, addCC);
            }
            if (bccTo != null && bccTo.length > 0) {
                InternetAddress[] addCC = new InternetAddress[bccTo.length];
                for (int ii = 0; ii < bccTo.length; ii++) {
                    InternetAddress ta = new InternetAddress(bccTo[ii]);
                    addCC[ii] = ta;
                }
                message.setRecipients(Message.RecipientType.BCC, addCC);
            }
            if (!Strings.isNullOrEmpty(subject)) {
                message.setSubject(subject);
            }
            message.setHeader(MessageWrapper.PARAM_MESSAGE_ID, UUID.randomUUID().toString());

            return new MessageWrapper(mailId, message);
        } catch (Exception e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends IEntity<?>> E createEntity(@NonNull E entity,
                                                 @NonNull Class<? extends E> entityClass,
                                                 Context context) throws DataStoreException {
        Preconditions.checkState(smtpConnection != null);
        Preconditions.checkArgument(entity instanceof MessageWrapper);
        Preconditions.checkArgument(ReflectionHelper.isSuperType(MessageWrapper.class, entityClass));
        try {
            MessageWrapper message = (MessageWrapper) entity;
            DefaultLogger.trace(String.format("Before Sending email... %s", smtpConnection));
            Transport.send(message.getMessage());

            return (E) message;
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    @Override
    public <E extends IEntity<?>> E updateEntity(@NonNull E entity,
                                                 @NonNull Class<? extends E> entityClass,
                                                 Context context) throws DataStoreException {
        return create(entity, entityClass, context);
    }

    @Override
    public <E extends IEntity<?>> boolean deleteEntity(@NonNull Object key,
                                                       @NonNull Class<? extends E> entityClass,
                                                       Context context) throws DataStoreException {
        MessageWrapper m = (MessageWrapper) findEntity(key, entityClass, true, context);
        if (m != null) {
            try {
                Message mail = m.getMessage();
                mail.setFlag(Flags.Flag.DELETED, true);

                mail.getFolder().expunge();
                return true;
            } catch (Exception ex) {
                throw new DataStoreException(ex);
            }
        }
        return false;
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public <E extends IEntity<?>> E findEntity(@NonNull Object key,
                                               @NonNull Class<? extends E> entityClass,
                                               boolean readOnly,
                                               Context context) throws DataStoreException {
        Preconditions.checkArgument(key instanceof String || key instanceof StringKey);
        Preconditions.checkArgument(ReflectionHelper.isSuperType(MessageWrapper.class, entityClass));
        try {
            Folder folder = folder(mailbox, false);
            String id = null;
            if (key instanceof String) {
                id = (String) key;
            } else {
                id = ((StringKey) key).getKey();
            }
            SearchTerm term = new MessageIDTerm(id);
            Message[] messages = folder.search(term);
            if (messages != null && messages.length > 0) {
                Message m = messages[0];
                return (E) new MessageWrapper(imapConnection.getEmailId(), m);
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <E extends IEntity<?>> BaseSearchResult<E> doSearch(@NonNull String query,
                                                               int offset,
                                                               int maxResults,
                                                               @NonNull Class<? extends E> entityClass,
                                                               Context context) throws DataStoreException {
        return doSearch(query, offset, maxResults, null, entityClass, context);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <E extends IEntity<?>> BaseSearchResult<E> doSearch(@NonNull String query,
                                                               int offset,
                                                               int maxResults,
                                                               Map<String, Object> parameters,
                                                               @NonNull Class<? extends E> entityClass,
                                                               Context context)
            throws DataStoreException {
        try {
            String foldername = getFolderName(query);
            if (Strings.isNullOrEmpty(foldername)) {
                foldername = mailbox;
            }
            String qstr = getQueryString(query);
            if (!Strings.isNullOrEmpty(qstr)) {
                SearchTerm st = parseQuery(qstr);

                Folder folder = folder(foldername, false);
                Message[] messages = folder.search(st);
                if (messages != null && messages.length > 0) {
                    List<MessageWrapper> response = new ArrayList<>(messages.length);
                    for (Message m : messages) {
                        MessageWrapper km = new MessageWrapper(imapConnection.getEmailId(), m);
                        if (km.getMessageId() == null || Strings.isNullOrEmpty(km.getMessageId().stringKey())) {
                            DefaultLogger.error(
                                    String.format("Invalid message received: Message ID is null. [mailbox=%s]",
                                            imapConnection().getEmailId()));
                            continue;
                        }
                        response.add(km);
                    }
                    if (maxResults <= 0)
                        maxResults = this.settings.getMaxResults();
                    Collection<MessageWrapper> collection = CollectionUtils.getRange(response, maxResults, offset);
                    if (collection != null && !collection.isEmpty()) {
                        EntitySearchResult<MessageWrapper> er = new EntitySearchResult<>(entityClass);
                        er.setQuery(query);
                        er.setOffset(offset);
                        er.setCount(collection.size());
                        er.setEntities(collection);

                        return (BaseSearchResult<E>) er;
                    }
                }
            } else {
                Collection<AbstractMailMessage<Message>> collection = fetch(foldername, maxResults, offset);
                if (collection != null && !collection.isEmpty()) {
                    EntitySearchResult<AbstractMailMessage<Message>> er = new EntitySearchResult<>(entityClass);
                    er.setQuery(query);
                    er.setOffset(offset);
                    er.setCount(collection.size());
                    er.setEntities(collection);

                    return (BaseSearchResult<E>) er;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public boolean isEmpty(@NonNull String folder) throws DataStoreException {
        try {
            Folder f = folder(folder, false);
            if (f != null) {
                return isEmpty(f);
            }
            return true;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public boolean isEmpty(@NonNull Folder folder) throws DataStoreException {
        try {
            Folder[] children = folder.list();
            if (children != null && children.length > 0) {
                for (Folder f : children) {
                    if (!isEmpty(f)) {
                        return false;
                    }
                }
            }
            if (folder.getMessageCount() == 0) {
                return true;
            } else if (DefaultLogger.isDebugEnabled()) {
                DefaultLogger.debug(
                        String.format("Folder [%s] Message Count: %d", folder.getFullName(), folder.getMessageCount()));
            }
            return false;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public Folder getFolder(@NonNull String name) throws DataStoreException {
        return folder(name, false);
    }

    private Folder folder(String name) throws DataStoreException {
        return folder(name, true);
    }

    private Folder folder(String name, boolean create) throws DataStoreException {
        try {
            Folder folder = imapConnection.connection().getFolder(name);
            if (folder == null) {
                throw new MessagingException(
                        String.format("[server:%s:%d][user=%s] Folder not " + "found. " + "[name=%s].",
                                imapConnection.getMailServer(), imapConnection.getPort(), imapConnection.getUsername(), name));
            }
            if (!folder.exists()) {
                if (create) {
                    DefaultLogger.debug(String.format("[server:%s:%d][user=%s] Creating folder " + "[name=%s].",
                            imapConnection.getMailServer(), imapConnection.getPort(), imapConnection.getUsername(), name));
                    folder = createFolder(name);
                } else {
                    throw new MessagingException(String.format(
                            "[server:%s:%d][user=%s] Folder not " + "found. " + "[name=%s].",
                            imapConnection.getMailServer(), imapConnection.getPort(), imapConnection.getUsername(), name));
                }
            }
            if (!folder.isOpen()) {
                folder.open(Folder.READ_WRITE);
                openedFolders.put(folder.getFullName(), folder);
            }
            return folder;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public Folder createFolder(@NonNull String name) throws DataStoreException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        try {
            if (name.indexOf('/') >= 0) {
                String[] parts = name.split("/");
                String path = null;
                Folder folder = null;
                for (String part : parts) {
                    if (Strings.isNullOrEmpty(part)) {
                        continue;
                    }
                    if (Strings.isNullOrEmpty(path)) {
                        folder = folder(part);
                        path = part;
                    } else {
                        path = String.format("%s/%s", path, part);
                        folder = imapConnection.connection().getFolder(path);
                        if (!folder.exists()) {
                            if (!folder.create(Folder.HOLDS_MESSAGES | Folder.HOLDS_FOLDERS)) {
                                throw new DataStoreException(
                                        String.format("Error creating folder. " + "[path=%s]", name));
                            }
                            folder.setSubscribed(true);
                        }
                    }
                }
                return folder;
            } else {
                Folder nf = imapConnection.connection().getFolder(name);
                if (!nf.exists()) {
                    if (!nf.create(Folder.HOLDS_MESSAGES | Folder.HOLDS_FOLDERS)) {
                        throw new DataStoreException(String.format("Error creating folder. " + "[path=%s]", name));
                    }
                    nf.setSubscribed(true);
                }
                return nf;
            }
        } catch (Exception e) {
            throw new DataStoreException(e);
        }
    }

    private String getFolderName(String query) throws ParseException {
        if (!Strings.isNullOrEmpty(query)) {
            String mbox = MailUtils.getMailBox(query);
            if (!Strings.isNullOrEmpty(mbox)) {
                return mbox;
            }
        }
        return DEFAULT_IMAP_FOLDER;
    }

    private String getQueryString(String query) throws ParseException {
        if (!Strings.isNullOrEmpty(query))
            return MailUtils.getMailQuery(query);
        return null;
    }

    private SearchTerm parseQuery(String query) throws Exception {
        MailRQLParser parser = new MailRQLParser();
        return parser.parse(query);
    }

    @Override
    public List<AbstractMailMessage<Message>> recover(String folder, boolean deleteEmpty) throws DataStoreException {
        try {
            List<AbstractMailMessage<Message>> messages = new ArrayList<>();
            Folder root = folder(folder, false);
            if (root != null) {
                Folder[] children = root.list();
                if (children != null && children.length > 0) {
                    for (Folder f : children) {
                        List<AbstractMailMessage<Message>> cms = recoverAndDelete(f.getFullName(), deleteEmpty);
                        if (cms != null && !cms.isEmpty()) {
                            messages.addAll(cms);
                        }
                    }
                }
            }
            Collection<AbstractMailMessage<Message>> ms = fetch(root.getFullName(), 0, -1);
            if (ms != null && !ms.isEmpty()) {
                for (AbstractMailMessage<Message> m : ms) {
                    move(m, mailbox);
                    messages.add(m);
                }
            }
            if (!messages.isEmpty())
                return messages;
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private List<AbstractMailMessage<Message>> recoverAndDelete(String folder, boolean deleteEmpty)
            throws DataStoreException {
        try {
            List<AbstractMailMessage<Message>> messages = new ArrayList<>();
            Folder root = folder(folder, false);
            if (root != null) {
                Folder[] children = root.list();
                if (children != null && children.length > 0) {
                    for (Folder f : children) {
                        List<AbstractMailMessage<Message>> cms = recoverAndDelete(f.getFullName(), deleteEmpty);
                        if (cms != null && !cms.isEmpty()) {
                            messages.addAll(cms);
                        }
                    }
                }
                Collection<AbstractMailMessage<Message>> ms = fetch(root.getFullName(), 0, -1);
                if (ms != null && !ms.isEmpty()) {
                    for (AbstractMailMessage<Message> m : ms) {
                        DefaultLogger.info(
                                String.format(
                                        "[RECOVER] Moving message to folder [%s]. [source folder=%s][message ID=%s]",
                                        root.getFullName(), mailbox, m.getMessageId().stringKey()));
                        move(m, mailbox);
                        messages.add(m);
                    }
                }
                root.close(true);
                if (deleteEmpty)
                    if (!root.delete(true)) {
                        DefaultLogger.warn(
                                String.format("Error deleting folder. [folder=%s]", root.getFullName()));
                    }
            }
            if (!messages.isEmpty())
                return messages;
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public Collection<AbstractMailMessage<Message>> fetch(String folderName, int batchSize, int offset)
            throws DataStoreException {
        try {
            Folder folder = folder(folderName, false);
            if (folder == null) {
                throw new DataStoreException(String.format("fetch call -> Folder not found. [folder=%s]", folderName));
            }
            if (offset <= 0) {
                offset = 1;
            }
            int mcount = folder.getMessageCount();
            if (offset > mcount)
                return null;

            int rsize = offset + batchSize;
            if (batchSize <= 0) {
                if (rsize > mcount) {
                    batchSize = mcount - offset;
                } else
                    batchSize = (Math.min(this.settings.getMaxResults(), mcount));
            } else {
                batchSize = Math.min(mcount, batchSize);
                if (rsize > mcount) {
                    batchSize = mcount - rsize;
                }
            }

            Message[] messages = folder.getMessages(offset, (offset + batchSize - 1));
            if (messages != null && messages.length > 0) {
                List<AbstractMailMessage<Message>> response = new ArrayList<>(messages.length);
                for (int ii = 0; ii < messages.length; ii++) {
                    Message m = messages[ii];
                    if (m.isExpunged() || m.isSet(Flags.Flag.DELETED)) {
                        DefaultLogger.warn(
                                String.format("Expunged/Deleted message received: Message ID is null. [mailbox=%s]",
                                        imapConnection().getEmailId()));
                        continue;
                    }
                    MessageWrapper km = new MessageWrapper(imapConnection.getEmailId(), m);
                    if (km.getMessageId() == null || Strings.isNullOrEmpty(km.getMessageId().stringKey())) {
                        DefaultLogger.error(
                                String.format("Invalid message received: Message ID is null. [mailbox=%s]",
                                        imapConnection().getEmailId()));
                        continue;
                    }
                    response.add(km);
                }
                if (batchSize <= 0)
                    batchSize = this.settings.getMaxResults();
                return CollectionUtils.getRange(response, batchSize, offset - 1);
            }
            return null;

        } catch (DataStoreException e) {
            throw e;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        try {
            long threadId = Thread.currentThread().getId();
            imapConnection.close(threadId);
            if (!openedFolders.isEmpty()) {
                for (String key : openedFolders.keySet()) {
                    Folder f = openedFolders.get(key);
                    if (f.isOpen()) {
                        f.close(true);
                    }
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void move(@NonNull AbstractMailMessage<Message> message, @NonNull String targetf) throws DataStoreException {
        Preconditions.checkArgument(message != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(targetf));

        try {
            Folder tf = folder(targetf);
            Folder sf = message.getMessage().getFolder();
            if (!sf.isOpen()) {
                sf.open(Folder.READ_WRITE);
                openedFolders.put(sf.getFullName(), sf);
            }
            sf.copyMessages(new Message[]{message.getMessage()}, tf);
            message.getMessage().setFlag(Flags.Flag.DELETED, true);
            sf.expunge();
        } catch (MessagingException e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    public void move(@NonNull AbstractMailMessage<Message>[] messages, @NonNull String targetf)
            throws DataStoreException {
        Preconditions.checkArgument(messages != null && messages.length > 0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(targetf));

        try {
            Folder tf = folder(targetf);
            Folder sf = messages[0].getMessage().getFolder();
            if (!sf.isOpen()) {
                sf.open(Folder.READ_WRITE);
                openedFolders.put(sf.getFullName(), sf);
            }
            Message[] ms = new Message[messages.length];
            for (int ii = 0; ii < messages.length; ii++) {
                ms[ii] = messages[ii].getMessage();
            }
            sf.copyMessages(ms, tf);
            for (Message message : ms) {
                message.setFlag(Flags.Flag.DELETED, true);
            }
            sf.expunge();
        } catch (MessagingException e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    public MessageWrapper readFromFile(@NonNull String path) throws DataStoreException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));

        File sf = new File(path);
        if (!sf.exists())
            throw new DataStoreException(String.format("Mail file not found. [path=%s]", path));
        try {
            InputStream fis = new FileInputStream(sf);
            MimeMessage message = new MimeMessage(smtpConnection.connection(), fis);
            return new MessageWrapper(imapConnection.getEmailId(), message);
        } catch (FileNotFoundException | MessagingException e) {
            throw new DataStoreException(e);
        }
    }

    public void deleteChildFolders(@NonNull String path) throws DataStoreException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        try {
            Folder folder = folder(path, false);
            Folder[] folders = folder.list();
            if (folders != null && folders.length > 0) {
                for (Folder f : folders) {
                    if (!deleteFolder(f.getFullName(), true)) {
                        DefaultLogger.error(String.format("Folder delete failed. [folder=%s]", f.getFullName()));
                    }
                }
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public boolean deleteFolder(@NonNull String path, boolean recursive) throws DataStoreException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        try {
            Folder folder = folder(path, false);
            DefaultLogger.debug(String.format("Deleting MailBox folder. [folder=%s]", folder.getFullName()));
            if (recursive) {
                Folder[] folders = folder.list();
                if (folders != null && folders.length > 0) {
                    for (Folder f : folders) {
                        if (!deleteFolder(f.getFullName(), recursive)) {
                            DefaultLogger.error(
                                    String.format("Folder delete failed. [folder=%s]", f.getFullName()));
                            return false;
                        }
                    }
                }
            }
            if (folder.exists()) {
                if (isEmpty(folder)) {
                    String fname = folder.getFullName();
                    folder.close(true);
                    boolean ret = folder.delete(true);
                    if (ret) {
                        DefaultLogger.info(String.format("Deleted Mailbox folder. [folder=%s]", fname));
                    } else {
                        DefaultLogger.warn(
                                String.format("Delete folder failed: call returned false. [folder=%s]", fname));
                    }
                    return ret;
                } else {
                    DefaultLogger.warn(String.format("Failed to delete folder: Folder isn't empty. [folder=%s]",
                            folder.getFullName()));
                }
            }
            return false;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public List<String> folders(String term) throws DataStoreException {
        Preconditions.checkState(imapConnection != null);
        try {
            Folder[] folders = imapConnection.connection().getDefaultFolder().list(term);
            if (folders != null && folders.length > 0) {
                List<String> names = new ArrayList<>();
                for (Folder folder : folders) {
                    names.add(folder.getName());
                }
                return names;
            }
            return null;
        } catch (Exception ex) {
            DefaultLogger.error(getClass().getCanonicalName(), ex);
            throw new DataStoreException(ex);
        }
    }

    @Override
    public Map<String, Integer> getFolderMessageCounts(String parent) throws DataStoreException {
        try {
            Folder folder = folder(parent);
            if (folder != null) {
                Map<String, Integer> counts = new HashMap<>();
                Folder[] children = folder.list();
                if (children != null && children.length > 0) {
                    for (Folder child : children) {
                        String fname = String.format("%s/%s", parent, child.getName());
                        DefaultLogger.debug(String.format("Checking folder : [%s]", fname));
                        Map<String, Integer> cc = getFolderMessageCounts(fname);
                        if (cc != null && !cc.isEmpty()) {
                            counts.putAll(cc);
                        }
                    }
                }
                int mc = folder.getMessageCount();
                if (mc > 0) {
                    DefaultLogger.debug(String.format("Folder message count : [%s][%d]", parent, mc));
                    counts.put(parent, mc);
                }
                if (!counts.isEmpty()) {
                    return counts;
                }
            }
            return null;
        } catch (Exception e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    public void cleanUpFolders(String parent) throws DataStoreException {
        try {
            Folder folder = folder(parent);
            if (folder != null) {
                DefaultLogger.warn(String.format("Cleaning folder [%s]...", parent));
                Folder[] children = folder.list();
                if (children != null && children.length > 0) {
                    for (Folder f : children) {
                        checkAndDelete(f);
                    }
                }
            }
        } catch (Exception e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    public void cleanUpEmptyFoldersBasedOnTimePeriod(String processingFolderPath, String folderPath, int timePeriod) throws DataStoreException {
        //TODO not yet implemented
    }

    private void checkAndDelete(Folder folder) throws DataStoreException, MessagingException {
        DefaultLogger.warn( String.format("Checking folder [%s]...", folder.getFullName()));
        Folder[] children = folder.list();
        if (children != null && children.length > 0) {
            for (Folder f : children) {
                checkAndDelete(f);
            }
        }
        if (isEmpty(folder)) {
            DefaultLogger.info( String.format("Deleting empty folder. [folder=%s]", folder.getFullName()));
            if (!deleteFolder(folder.getFullName(), true)) {
                DefaultLogger.error( String.format("Failed to delete folder. [folder=%s]", folder.getFullName()));
            }
        } else {
            DefaultLogger.info(String.format("Folder is not empty. [folder=%s]", folder.getFullName()));
        }
    }
}
