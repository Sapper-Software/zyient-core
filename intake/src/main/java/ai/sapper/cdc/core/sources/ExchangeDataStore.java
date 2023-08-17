package ai.sapper.cdc.core.sources;

import com.codekutter.common.Context;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.StringKey;
import com.codekutter.common.stores.BaseSearchResult;
import com.codekutter.common.stores.DataStoreException;
import com.codekutter.common.stores.DataStoreManager;
import com.codekutter.common.stores.impl.DataStoreAuditContext;
import com.codekutter.common.stores.impl.EntitySearchResult;
import com.codekutter.common.utils.IOUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.ingestion.common.connections.ExchangeConnection;
import com.ingestion.common.ext.utils.MailUtils;
import com.ingestion.common.model.AbstractMailMessage;
import com.ingestion.common.model.EmailMessageWrapper;
import com.ingestion.common.model.MailDataSourceDbConfig;
import com.ingestion.common.model.MessageWrapper;
import com.ingestion.common.utils.DateUtils;
import com.vdurmont.emoji.EmojiParser;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import microsoft.exchange.webservices.data.core.ExchangeService;
import microsoft.exchange.webservices.data.core.PropertySet;
import microsoft.exchange.webservices.data.core.enumeration.property.BasePropertySet;
import microsoft.exchange.webservices.data.core.enumeration.property.MapiPropertyType;
import microsoft.exchange.webservices.data.core.enumeration.property.WellKnownFolderName;
import microsoft.exchange.webservices.data.core.enumeration.search.FolderTraversal;
import microsoft.exchange.webservices.data.core.enumeration.search.SortDirection;
import microsoft.exchange.webservices.data.core.enumeration.service.DeleteMode;
import microsoft.exchange.webservices.data.core.exception.service.local.ServiceLocalException;
import microsoft.exchange.webservices.data.core.service.folder.Folder;
import microsoft.exchange.webservices.data.core.service.item.EmailMessage;
import microsoft.exchange.webservices.data.core.service.item.Item;
import microsoft.exchange.webservices.data.core.service.schema.EmailMessageSchema;
import microsoft.exchange.webservices.data.core.service.schema.FolderSchema;
import microsoft.exchange.webservices.data.core.service.schema.ItemSchema;
import microsoft.exchange.webservices.data.property.complex.*;
import microsoft.exchange.webservices.data.property.definition.ExtendedPropertyDefinition;
import microsoft.exchange.webservices.data.search.FindFoldersResults;
import microsoft.exchange.webservices.data.search.FindItemsResults;
import microsoft.exchange.webservices.data.search.FolderView;
import microsoft.exchange.webservices.data.search.ItemView;
import microsoft.exchange.webservices.data.search.filter.SearchFilter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import javax.annotation.Nonnull;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
@Accessors(fluent = true)
@SuppressWarnings({"rawtypes", "unchecked"})
public class ExchangeDataStore extends AbstractMailDataStore<ExchangeService, EmailMessage, Folder> {
    private static final int DEFAULT_SEND_CHECK_TIMEOUT = 60 * 1000;
    private static final String queryRegex = "\\{(.*)}:\\{(.*)}";
    public static final String DEFAULT_IMAP_FOLDER = "Inbox";
    private static final UUID pvaiExtendedPropertyId = UUID.randomUUID();

    @Setter(AccessLevel.NONE)
    private ExchangeConnection exchangeConnection;
    @Setter(AccessLevel.NONE)
    private WellKnownFolderName rootFolder;
    @Setter(AccessLevel.NONE)
    private Session jMailSession;
    @Setter(AccessLevel.NONE)
    private ExtendedPropertyDefinition extendedPropertyDefinition;

    @ConfigAttribute(name = "xml")
    private String XML_REPLACE_REGEX;
    @ConfigAttribute(name = "name")
    private String XML_REPLACE_CHAR;

    @Override
    public String getMailUser() {
        if (exchangeConnection != null) {
            return exchangeConnection.emailId();
        }
        return null;
    }

    @Override
    public EmailMessage createMessage(@Nonnull String mailId, @Nonnull com.ingestion.common.model.EmailMessage email,
                                      @Nonnull String sender) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        try {
            EmailMessage message = new EmailMessage(exchangeConnection.connection());
            message.setFrom(new EmailAddress(mailId));
            if (email.getHeader().getTo() != null && email.getHeader().getTo().length > 0) {
                for (String address : email.getHeader().getTo()) {
                    message.getToRecipients().add(address);
                }
            }
            if (email.getHeader().getCc() != null && email.getHeader().getCc().length > 0) {
                for (String address : email.getHeader().getCc()) {
                    message.getCcRecipients().add(address);
                }
            }
            if (email.getHeader().getBcc() != null && email.getHeader().getBcc().length > 0) {
                for (String address : email.getHeader().getBcc()) {
                    message.getBccRecipients().add(address);
                }
            }
            if (email.getHeader().getReplyTo() != null && email.getHeader().getReplyTo().length > 0) {
                for (String address : email.getHeader().getReplyTo()) {
                    message.getReplyTo().add(address);
                }
            }
            message.setSubject(email.getHeader().getSubject());
            if (!Strings.isNullOrEmpty(email.getBody())) {
                String bodyString = EmojiParser.parseToUnicode(email.getBody());
                bodyString = EmojiParser.removeAllEmojis(bodyString);
                setMessageBody(message, bodyString);
            }
            if (email.getAttachments() != null && !email.getAttachments().isEmpty()) {
                for (String attachment : email.getAttachments()) {
                    message.getAttachments().addFileAttachment(attachment);
                }
            }

            return message;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private void setMessageBody(EmailMessage message, String body) throws Exception {
        Document doc = Jsoup.parse(body);
        Elements elems = doc.select("img");
        int count = 0;
        if (elems != null && elems.size() > 0) {
            for (Element elm : elems) {
                Elements celems = elm.select("[src~=data:.*]");
                if (celems == null || celems.size() < 0)
                    continue;
                MimeContent content = saveImageToLocal(elm, count);
                if (content != null) {
                    File source = content.getLocalFile();
                    if (source == null)
                        continue;
                    if (!source.exists()) {
                        throw new DataStoreException("Error creating image file...");
                    }
                    elm.attr("src", String.format("cid:%s", source.getName()));
                    FileAttachment attch = message.getAttachments().addFileAttachment(source.getAbsolutePath());
                    attch.setIsInline(true);
                    attch.setContentId(source.getName());
                    attch.setContentType(content.getMimeType());

                    count++;
                }
            }
            body = doc.toString();
        }
        MessageBody bd = MessageBody.getMessageBodyFromText(body);
        message.setBody(bd);
    }

    private MimeContent saveImageToLocal(Element node, int index) throws Exception {
        String data = node.attr("src");
        if (!Strings.isNullOrEmpty(data)) {
            MimeContent content = MailUtils.extractInlineContent(data);
            if (content != null) {
                String fname = MailUtils.getFileNameWithExtension(content.getMimeType(), "InlineAttachment_" + index);
                String dir = IOUtils.getTempDirectory();
                String path = String.format("%s/%s", dir, fname);
                File fi = new File(path);
                if (fi.exists()) {
                    fi.delete();
                }
                try (FileOutputStream fos = new FileOutputStream(fi)) {
                    fos.write(content.getContent());
                }
                content.setLocalFile(fi);

                return content;
            }
        }
        return null;
    }

    @Override
    public AbstractMailMessage<EmailMessage> createWrappedMessage(@Nonnull String mailId,
                                                                  @Nonnull com.ingestion.common.model.EmailMessage email, @Nonnull String sender) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        EmailMessage message = createMessage(mailId, email, sender);
        if (message != null) {
            try {
                String mid = UUID.randomUUID().toString();
                message.setExtendedProperty(extendedPropertyDefinition, mid);
                EmailMessageWrapper m = new EmailMessageWrapper(mailId, message, false);
                m.setPvaiCorrelationId(mid);
                return m;
            } catch (Exception ex) {
                throw new DataStoreException(ex);
            }
        }
        return null;
    }

    @Override
    public AbstractMailMessage<EmailMessage> createMessage(@Nonnull String mailId, @Nonnull String sender,
                                                           @Nonnull String[] sendTo, String[] ccTo, String[] bccTo, String subject) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        try {
            EmailMessage message = new EmailMessage(exchangeConnection.connection());
            message.setFrom(new EmailAddress(sender));
            if (sendTo != null && sendTo.length > 0) {
                for (String address : sendTo) {
                    message.getToRecipients().add(address);
                }
            }
            if (ccTo != null && ccTo.length > 0) {
                for (String address : ccTo) {
                    message.getCcRecipients().add(address);
                }
            }
            if (bccTo != null && bccTo.length > 0) {
                for (String address : bccTo) {
                    message.getBccRecipients().add(address);
                }
            }

            message.setSubject(subject);
            String mid = UUID.randomUUID().toString();
            message.setExtendedProperty(extendedPropertyDefinition, mid);
            EmailMessageWrapper m = new EmailMessageWrapper(mailId, message, false);
            m.setPvaiCorrelationId(mid);
            return m;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public boolean isEmpty(@Nonnull String folder) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        Folder f = getFolder(folder);
        if (f == null) {
            throw new DataStoreException(String.format("isEmpty call -> Folder not found. [name=%s]", folder));
        }
        return isEmpty(f);
    }

    @Override
    public boolean isEmpty(@Nonnull Folder folder) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        try {
            ItemView view = new ItemView(1);
            FindItemsResults<Item> findResults = exchangeConnection.connection().findItems(folder.getId(), view);
            if (findResults == null || findResults.getItems() == null) {
                Folder[] children = findChildFolders(folder, FolderTraversal.Shallow);
                return (children == null || children.length <= 0);
            }
            return findResults.getItems().isEmpty();
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public Folder getFolder(@Nonnull String name) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        return folder(name, false);
    }

    private Folder getSendFolder() throws DataStoreException {
        String path = DateUtils.formatTimestamp("yyyy/MM/dd/HH");
        return folder(WellKnownFolderName.SentItems, path, true);
    }

    private Folder folder(WellKnownFolderName root, String name) throws DataStoreException {
        return folder(root, name, true);
    }

    private Folder folder(String name, boolean create) throws DataStoreException {
        return folder(rootFolder, name, create);
    }

    private Folder folder(WellKnownFolderName root, String name, boolean create) throws DataStoreException {
        try {
            String[] parts = name.split("/");
            if (parts != null && parts.length > 0) {
                return findFolder(root, null, parts, 0, create);
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private Folder findFolder(WellKnownFolderName root, Folder parent, String[] parts, int index, boolean create)
            throws DataStoreException {
        try {
            String name = parts[index];
            Folder folder = null;
            if (parent == null) {
                WellKnownFolderName kf = checkKnownFolder(name);
                if (kf != null) {
                    FolderId fid = null;
                    if (exchangeConnection.useDelegate()) {
                        fid = new FolderId(kf, new Mailbox(exchangeConnection.delegateUser()));
                        folder = Folder.bind(exchangeConnection.connection(), new FolderId(kf, new Mailbox(exchangeConnection.delegateUser())));
                    } else {
                        fid = new FolderId(kf);
                        folder = Folder.bind(exchangeConnection.connection(), fid);
                    }

                    if (parts.length == 1) {
                        return folder;
                    }
                    return findFolder(kf, folder, parts, index + 1, create);
                } else {
                    FolderId fid = null;
                    FolderView fView = new FolderView(1);
                    fView.setTraversal(FolderTraversal.Shallow);
                    SearchFilter filter = new SearchFilter.IsEqualTo(FolderSchema.DisplayName, name);
                    if (exchangeConnection.useDelegate()) {
                        fid = new FolderId(root, new Mailbox(exchangeConnection.delegateUser()));
                    } else {
                        fid = new FolderId(root);
                    }
                    FindFoldersResults results = exchangeConnection.connection().findFolders(fid, filter, fView);
                    if (results != null && results.getFolders().size() > 0) {
                        folder = results.getFolders().get(0);
                    } else if (create) {
                        folder = new Folder(exchangeConnection.connection());
                        folder.setDisplayName(name);
                        if (exchangeConnection.useDelegate()) {
                            FolderId fi = new FolderId(root, new Mailbox(exchangeConnection.delegateUser()));
                            folder.save(fi);
                        } else
                            folder.save(root);
                    }
                }
            } else {
                FolderView fView = new FolderView(1);
                fView.setTraversal(FolderTraversal.Shallow);
                SearchFilter filter = new SearchFilter.IsEqualTo(FolderSchema.DisplayName, name);
                FindFoldersResults results = exchangeConnection.connection().findFolders(parent.getId(), filter, fView);
                if (results != null && results.getFolders().size() > 0) {
                    folder = results.getFolders().get(0);
                } else if (create) {
                    folder = new Folder(exchangeConnection.connection());
                    folder.setDisplayName(name);
                    folder.save(parent.getId());
                }
            }
            if (folder != null) {
                if (index == parts.length - 1) {
                    return folder;
                } else {
                    return findFolder(root, folder, parts, index + 1, create);
                }
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private WellKnownFolderName checkKnownFolder(String name) {
        for (WellKnownFolderName f : WellKnownFolderName.values()) {
            if (f.name().compareTo(name) == 0) {
                return f;
            }
        }
        return null;
    }

    private Folder createFolder(Folder parent, String[] parts, int index) throws DataStoreException {
        try {
            Folder root = parent;
            if (root == null) {
                if (exchangeConnection.useDelegate()) {
                    FolderId fi = new FolderId(rootFolder, new Mailbox(exchangeConnection.delegateUser()));
                    root = Folder.bind(exchangeConnection.connection(), fi);
                } else
                    root = Folder.bind(exchangeConnection.connection(), rootFolder);
            }
            String name = parts[index];
            FolderView fView = new FolderView(1);
            fView.setTraversal(FolderTraversal.Shallow);
            SearchFilter filter = new SearchFilter.IsEqualTo(FolderSchema.DisplayName, name);
            FindFoldersResults results = exchangeConnection.connection().findFolders(root.getId(), filter, fView);
            if (results.getFolders() != null && !results.getFolders().isEmpty()) {
                Folder folder = results.getFolders().get(0);
                if (index == parts.length - 1) {
                    return folder;
                } else {
                    return createFolder(folder, parts, index + 1);
                }
            } else {
                Folder folder = new Folder(exchangeConnection.connection());
                folder.setDisplayName(name);
                folder.save(root.getId());
                if (index == parts.length - 1) {
                    return folder;
                } else {
                    return createFolder(folder, parts, index + 1);
                }
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public Folder createFolder(@Nonnull String name) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Folder parent = null;
        try {
            String[] parts = name.split("/");
            if (parts.length == 1) {
                return createFolder(null, parts, 0);
            } else {
                String rootf = parts[0];
                WellKnownFolderName rf = WellKnownFolderName.valueOf(rootf);
                if (exchangeConnection.useDelegate()) {
                    parent = Folder.bind(exchangeConnection.connection(), new FolderId(rf, new Mailbox(exchangeConnection.delegateUser())));
                } else {
                    parent = Folder.bind(exchangeConnection.connection(), rf);
                }
                return createFolder(parent, parts, 1);
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public List<AbstractMailMessage<EmailMessage>> recover(String folder, boolean deleteEmpty)
            throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        Folder f = folder(folder, false);
        if (f == null) {
            throw new DataStoreException(String.format("recover call ->Folder not found. [folder=%s]", folder));
        }
        return recover(f, deleteEmpty);
    }

    private List<AbstractMailMessage<EmailMessage>> recover(Folder folder, boolean deleteEmpty)
            throws DataStoreException {
        try {
            List<AbstractMailMessage<EmailMessage>> messages = new ArrayList<>();
            if (folder != null) {
                Folder[] children = findChildFolders(folder, FolderTraversal.Shallow);
                if (children != null && children.length > 0) {
                    for (Folder f : children) {
                        List<AbstractMailMessage<EmailMessage>> cms = recover(f, deleteEmpty);
                        if (cms != null && !cms.isEmpty()) {
                            messages.addAll(cms);
                        }
                        Collection<AbstractMailMessage<EmailMessage>> ms = fetch(f, -1, 0);
                        if (ms != null && !ms.isEmpty()) {
                            for (AbstractMailMessage<EmailMessage> m : ms) {
                                move(m, mailbox);
                                messages.add(m);
                            }
                        }
                        if (deleteEmpty)
                            deleteFolder(f);
                    }
                }
                int mc = folder.getTotalCount();
                if (mc > 0) {
                    Collection<AbstractMailMessage<EmailMessage>> ms = fetch(folder, -1, 0);
                    if (ms != null && !ms.isEmpty()) {
                        for (AbstractMailMessage<EmailMessage> m : ms) {
                            move(m, mailbox);
                            messages.add(m);
                        }
                    }
                    if (deleteEmpty)
                        deleteFolder(folder);
                }
            }
            if (!messages.isEmpty())
                return messages;
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private Folder[] findChildFolders(Folder parent, FolderTraversal traversal) throws DataStoreException {
        try {
            FolderView fv = new FolderView(Integer.MAX_VALUE);
            fv.setTraversal(traversal);
            FindFoldersResults findResults = exchangeConnection.connection().findFolders(parent.getId(), fv);

            if (findResults.getFolders() != null && !findResults.getFolders().isEmpty()) {
                Folder[] folders = new Folder[findResults.getFolders().size()];
                int ii = 0;
                for (Folder folder : findResults.getFolders()) {
                    folders[ii] = folder;
                    ii++;
                }
                return folders;
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public Collection<AbstractMailMessage<EmailMessage>> fetch(String folderName, int batchSize, int offset)
            throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        if (Strings.isNullOrEmpty(folderName)) {
            return fetch(batchSize, offset);
        }
        Folder f = folder(folderName, false);
        if (f == null) {
            throw new DataStoreException(String.format("fetch call -> Folder not found. [folder=%s]", folderName));
        }
        return fetch(f, batchSize, offset);
    }

    private Collection<AbstractMailMessage<EmailMessage>> fetch(int batchSize, int offset) throws DataStoreException {
        try {
            if (offset < 0)
                offset = 0;
            if (batchSize <= 0)
                batchSize = maxResults();
            ItemView view = new ItemView(batchSize);
            view.getOrderBy().add(ItemSchema.DateTimeReceived, SortDirection.Ascending);
            view.setOffset(offset);

            FindItemsResults<Item> findResults = null;
            if (exchangeConnection.useDelegate()) {
                FolderId fi = new FolderId(rootFolder, new Mailbox(exchangeConnection.delegateUser()));
                findResults = exchangeConnection.connection().findItems(fi, view);
            } else
                findResults = exchangeConnection.connection().findItems(rootFolder, view);

            // MOOOOOOST IMPORTANT: load items properties, before
            exchangeConnection.connection().loadPropertiesForItems(findResults, PropertySet.FirstClassProperties);
            if (findResults.getItems() != null && !findResults.getItems().isEmpty()) {
                List<AbstractMailMessage<EmailMessage>> messages = new ArrayList<>(findResults.getItems().size());
                for (Item item : findResults) {
                    if (item instanceof EmailMessage) {
                        EmailMessage em = (EmailMessage) item;
                        EmailMessageWrapper ew = new EmailMessageWrapper(exchangeConnection.emailId(), em, true);
                        messages.add(ew);
                    }
                }
                return messages;
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private Collection<AbstractMailMessage<EmailMessage>> fetch(Folder folder, int batchSize, int offset)
            throws DataStoreException {
        try {
            if (offset < 0)
                offset = 0;
            if (batchSize <= 0)
                batchSize = maxResults();
            ItemView view = new ItemView(batchSize);
            view.getOrderBy().add(ItemSchema.DateTimeReceived, SortDirection.Descending);
            view.setOffset(offset);

            FindItemsResults<Item> findResults = exchangeConnection.connection().findItems(folder.getId(), view);

            if (findResults.getItems() != null && !findResults.getItems().isEmpty()) {
                // MOOOOOOST IMPORTANT: load items properties, before
                exchangeConnection.connection().loadPropertiesForItems(findResults, getPropertySet());
                List<AbstractMailMessage<EmailMessage>> messages = new ArrayList<>(findResults.getItems().size());
                for (Item item : findResults) {
                    if (item instanceof EmailMessage) {
                        EmailMessage em = (EmailMessage) item;
                        EmailMessageWrapper ew = new EmailMessageWrapper(exchangeConnection.emailId(), em, true);
                        messages.add(ew);
                    }
                }
                return messages;
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public void move(@Nonnull AbstractMailMessage<EmailMessage> message, @Nonnull String targetf)
            throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        Folder folder = folder(targetf, false);
        if (folder == null) {
            throw new DataStoreException(String.format("move msg call -> Folder not found. [folder=%s]", targetf));
        }
        try {
            exchangeConnection.connection().moveItem(message.getMessage().getId(), folder.getId());
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public void move(@Nonnull AbstractMailMessage<EmailMessage>[] messages, @Nonnull String targetf)
            throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        Folder folder = folder(targetf, false);
        if (folder == null) {
            throw new DataStoreException(String.format("move msgs call -> Folder not found. [folder=%s]", targetf));
        }
        try {
            for (AbstractMailMessage<EmailMessage> message : messages)
                exchangeConnection.connection().moveItem(message.getMessage().getId(), folder.getId());
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public AbstractMailMessage<EmailMessage> readFromFile(@Nonnull String path) throws DataStoreException {
        throw new DataStoreException("Method not implemented.");
    }

    // Hacked as EWS doesn't handle .eml files.
    public MessageWrapper readEmailFile(@Nonnull String path) throws DataStoreException {
        Preconditions.checkState(jMailSession != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));

        File sf = new File(path);
        if (!sf.exists())
            throw new DataStoreException(String.format("Mail file not found. [path=%s]", path));
        try {
            InputStream fis = new FileInputStream(sf);
            MimeMessage message = new MimeMessage(jMailSession, fis);
            return new MessageWrapper(exchangeConnection.emailId(), message);
        } catch (FileNotFoundException | MessagingException e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    public void deleteChildFolders(@Nonnull String path) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        Folder folder = folder(path, false);
        if (folder == null) {
            throw new DataStoreException(String.format("deleteChildFolders call -> Folder not found. [folder=%s]", path));
        }
        deleteChildFolders(folder);
    }

    private boolean deleteChildFolders(Folder folder) throws DataStoreException {
        Folder[] children = findChildFolders(folder, FolderTraversal.Shallow);
        if (children != null && children.length > 0) {
            boolean deleted = true;
            for (Folder f : children) {
                if (deleteChildFolders(f)) {
                    return deleteFolder(f);
                } else {
                    deleted = false;
                }
            }
            return deleted;
        }
        return true;
    }

    @Override
    public boolean deleteFolder(@Nonnull String path, boolean recursive) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        Folder folder = folder(path, false);
        if (folder == null) {
            throw new DataStoreException(String.format("deleteFolder call -> Folder not found. [folder=%s]", path));
        }
        deleteFolder(folder);
        return true;
    }

    private boolean deleteFolder(Folder folder) throws DataStoreException {
        try {
            if (isEmpty(folder)) {
                exchangeConnection.connection().deleteFolder(folder.getId(), DeleteMode.HardDelete);
                return true;
            }
            return false;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public List<String> folders(String term) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        try {
            FolderView fv = new FolderView(Integer.MAX_VALUE);
            fv.setTraversal(FolderTraversal.Deep);
            FindFoldersResults findResults = null;
            if (exchangeConnection.useDelegate()) {
                FolderId fi = new FolderId(rootFolder, new Mailbox(exchangeConnection.delegateUser()));
                findResults = exchangeConnection.connection().findFolders(fi,
                        new SearchFilter.IsEqualTo(FolderSchema.DisplayName, term), fv);
            } else
                findResults = exchangeConnection.connection().findFolders(rootFolder,
                        new SearchFilter.IsEqualTo(FolderSchema.DisplayName, term), fv);
            if (findResults.getFolders() != null && !findResults.getFolders().isEmpty()) {
                List<String> folders = new ArrayList<>();
                for (Folder folder : findResults) {
                    folders.add(folder.getDisplayName());
                }
                return folders;
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public void configureDataStore(@Nonnull DataStoreManager dataStoreManager) throws ConfigurationException {
        Preconditions.checkArgument(config() instanceof MailDataSourceDbConfig);
        try {
            exchangeConnection = (ExchangeConnection) dataStoreManager.getConnection(config().getConnectionName(),
                    ExchangeService.class);
            withConnection(exchangeConnection);
            if (!Strings.isNullOrEmpty(((MailDataSourceDbConfig) config()).getMailbox())) {
                mailbox = ((MailDataSourceDbConfig) config()).getMailbox();
            } else {
                mailbox = WellKnownFolderName.Inbox.name();
            }
            rootFolder = WellKnownFolderName.valueOf(mailbox);
            Properties props = new Properties();
            jMailSession = Session.getDefaultInstance(props);
            jMailSession.getProperties().setProperty("mail.mime.address.strict", "false");
            if (config().getMaxResults() > 0) {
                maxResults(config().getMaxResults());
            } else {
                maxResults(DEFAULT_MAX_RESULTS);
            }
            extendedPropertyDefinition = new ExtendedPropertyDefinition(pvaiExtendedPropertyId,
                    EmailMessageWrapper.EMAIL_CORRELATION_ID, MapiPropertyType.String);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public <E extends IEntity> E createEntity(@Nonnull E entity, @Nonnull Class<? extends E> entityType,
                                              Context context) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        Preconditions.checkArgument(ReflectionUtils.isSuperType(EmailMessageWrapper.class, entityType));
        try {
            EmailMessageWrapper message = (EmailMessageWrapper) entity;
            if (message.getMessage() == null) {
                throw new DataStoreException("Null message...");
            }

            String mid = message.getPvaiCorrelationId();
            if (Strings.isNullOrEmpty(mid)) {
                throw new DataStoreException(
                        String.format("Missing PVAI correlation ID. [message ID=%s]", message.getMessageId()));
            }
            Folder sentFolder = getSendFolder();
            message.getMessage().sendAndSaveCopy(sentFolder.getId());
            LogUtils.debug(getClass(),
                    String.format("Saving to sent folder. [path=%s][ID=%s]", sentFolder.getDisplayName(), mid));
            checkSendResponse(sentFolder, mid, message);

            return (E) message;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private void checkSendResponse(Folder folder, String mid, EmailMessageWrapper message) throws DataStoreException {
        try {
            long startTime = System.currentTimeMillis();
            while ((System.currentTimeMillis() - startTime) < DEFAULT_SEND_CHECK_TIMEOUT) {
                ItemView view2 = new ItemView(1);
                SearchFilter sf = new SearchFilter.IsEqualTo(extendedPropertyDefinition, mid);
                FindItemsResults<Item> messages = exchangeConnection.connection().findItems(folder.getId(), sf, view2);
                if (messages.getItems() != null && !messages.getItems().isEmpty()) {
                    exchangeConnection.connection().loadPropertiesForItems(messages, getPropertySet());
                    for (Item item : messages.getItems()) {
                        message.setMessage((EmailMessage) item);
                        return;
                    }
                } else {
                    Thread.sleep(500);
                }
            }
            throw new DataStoreException("Failed to validate send. Response not received...");
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <E extends IEntity> E updateEntity(@Nonnull E entity, @Nonnull Class<? extends E> entityType,
                                              Context context) throws DataStoreException {
        return createEntity(entity, entityType, context);
    }

    @Override
    public <E extends IEntity> boolean deleteEntity(@Nonnull Object key, @Nonnull Class<? extends E> entityType,
                                                    Context context) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        EmailMessageWrapper email = (EmailMessageWrapper) findEntity(key, entityType, context);
        try {
            if (email != null) {
                email.getMessage().delete(DeleteMode.MoveToDeletedItems);
                return true;
            }
            return false;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <E extends IEntity> E findEntity(@Nonnull Object key, @Nonnull Class<? extends E> entityType,
                                            Context context) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        Preconditions.checkArgument(key instanceof String || key instanceof StringKey);
        Preconditions.checkArgument(ReflectionUtils.isSuperType(EmailMessageWrapper.class, entityType));
        String id = null;
        if (key instanceof String) {
            id = (String) key;
        } else {
            id = ((StringKey) key).getKey();
        }
        try {
            String fname = getMailBox(id);
            Folder folder = null;
            if (!Strings.isNullOrEmpty(fname)) {
                folder = folder(fname, false);
                if (folder == null) {
                    throw new DataStoreException(String.format("findEntity call -> Folder not found. [folder=%s]", fname));
                }
            } else {
                if (exchangeConnection.useDelegate()) {
                    folder = Folder.bind(exchangeConnection.connection(), new FolderId(rootFolder, new Mailbox(exchangeConnection.delegateUser())));
                } else
                    folder = Folder.bind(exchangeConnection.connection(), new FolderId(rootFolder));
            }
            String query = getMailQuery(id);
            ItemView view = new ItemView(1);
            SearchFilter.IsEqualTo filter = new SearchFilter.IsEqualTo(EmailMessageSchema.InternetMessageId, query);
            FindItemsResults<Item> findResults = exchangeConnection.connection().findItems(folder.getId(), filter,
                    view);
            if (findResults.getItems() != null && !findResults.getItems().isEmpty()) {
                exchangeConnection.connection().loadPropertiesForItems(findResults, getPropertySet());
                Item item = findResults.getItems().get(0);
                if (item instanceof EmailMessage) {
                    return (E) new EmailMessageWrapper(exchangeConnection.emailId(), (EmailMessage) item, true);
                }
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <E extends IEntity> BaseSearchResult<E> doSearch(@Nonnull String queryStr, int offset, int maxResults,
                                                            @Nonnull Class<? extends E> entityType, Context context) throws DataStoreException {
        Preconditions.checkState(exchangeConnection != null);
        Collection<AbstractMailMessage<EmailMessage>> messages = null;

        try {
            String fname = getMailBox(queryStr);
            Folder folder = null;
            if (!Strings.isNullOrEmpty(fname)) {
                folder = folder(fname, false);
                if (folder == null) {
                    throw new DataStoreException(String.format("doSearch call -> Folder not found. [folder=%s]", fname));
                }
            } else {
                if (exchangeConnection.useDelegate()) {
                    folder = Folder.bind(exchangeConnection.connection(), new FolderId(rootFolder, new Mailbox(exchangeConnection.delegateUser())));
                } else
                    folder = Folder.bind(exchangeConnection.connection(), new FolderId(rootFolder));
            }
            String query = getMailQuery(queryStr);
            if (Strings.isNullOrEmpty(query)) {
                messages = fetch(folder, maxResults, offset);
            } else {
                if (maxResults <= 0)
                    maxResults = maxResults();
                ItemView view = new ItemView(maxResults);
                view.getOrderBy().add(ItemSchema.DateTimeReceived, SortDirection.Descending);

                FindItemsResults<Item> findResults = exchangeConnection.connection().findItems(folder.getId(), query,
                        view);
                if (findResults.getItems() != null && !findResults.getItems().isEmpty()) {
                    exchangeConnection.connection().loadPropertiesForItems(findResults, getPropertySet());
                    messages = new ArrayList<>();
                    for (Item item : findResults) {
                        if (item instanceof EmailMessage) {
                            EmailMessage em = (EmailMessage) item;
                            EmailMessageWrapper ew = new EmailMessageWrapper(exchangeConnection.emailId(), em, true);
                            messages.add(ew);
                        }
                    }
                }
            }
            if (messages != null && !messages.isEmpty()) {
                EntitySearchResult<AbstractMailMessage<EmailMessage>> er = new EntitySearchResult<>(entityType);
                er.setQuery(query);
                er.setOffset(offset);
                er.setCount(messages.size());
                er.setEntities(messages);

                return (BaseSearchResult<E>) er;
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public File writeToFile(@Nonnull EmailMessageWrapper message, @Nonnull String outputDir) throws DataStoreException {
        Preconditions.checkArgument(message.getMessage() != null);
        Preconditions.checkState(exchangeConnection != null);
        try {
            File dir = new File(outputDir);
            if (!dir.exists()) {
                throw new DataStoreException(String.format("writeToFile Directory not found. [path=%s]", dir.getAbsolutePath()));
            }
            String fname = String.format("%s.eml", message.getMessageId().getKey());
            File outf = new File(String.format("%s/%s", dir.getAbsolutePath(), fname));
            try (FileOutputStream fos = new FileOutputStream(outf)) {
                fos.write(message.getMessage().getMimeContent().getContent());
            }
            return outf;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public <E extends IEntity> BaseSearchResult<E> doSearch(@Nonnull String query, int offset, int maxResults,
                                                            Map<String, Object> map, @Nonnull Class<? extends E> entityType, Context context)
            throws DataStoreException {
        return doSearch(query, offset, maxResults, entityType, context);
    }

    public static PropertySet getPropertySet() throws Exception {
        BasePropertySet def = BasePropertySet.FirstClassProperties;
        PropertySet ps = new PropertySet(def);

        ps.add(EmailMessageSchema.HasAttachments);
        ps.add(ItemSchema.MimeContent);
        ps.add(EmailMessageSchema.Attachments);
        ps.add(EmailMessageSchema.InternetMessageId);
        ps.add(ItemSchema.Body);

        return ps;
    }

    @Override
    public DataStoreAuditContext context() {
        Preconditions.checkState(exchangeConnection != null);
        MailStoreAuditContext ctx = new MailStoreAuditContext();
        ctx.setType(getClass().getCanonicalName());
        ctx.setName(name());
        ctx.setConnectionType(connection().getClass().getCanonicalName());
        ctx.setConnectionName(connection().name());
        ctx.setMailbox(exchangeConnection.username());
        ctx.setFolder(mailbox);

        return ctx;
    }

    public static String getMailBox(@Nonnull String query) throws ParseException {
        try {
            if (!Strings.isNullOrEmpty(query)) {
                Pattern pattern = Pattern.compile(queryRegex);
                Matcher m = pattern.matcher(query);
                if (m != null && m.matches()) {
                    if (m.groupCount() > 1) {
                        return m.group(1);
                    }
                }
            }
            return null;
        } catch (Exception ex) {
            throw new ParseException(ex.getLocalizedMessage(), 0);
        }
    }

    public static String getMailQuery(@Nonnull String query) throws ParseException {
        try {
            if (!Strings.isNullOrEmpty(query)) {
                Pattern pattern = Pattern.compile(queryRegex);
                Matcher m = pattern.matcher(query);
                if (m != null && m.matches()) {
                    if (m.groupCount() > 1) {
                        return m.group(2);
                    }
                }
            }
            return query;
        } catch (Exception ex) {
            throw new ParseException(ex.getLocalizedMessage(), 0);
        }
    }

    public static String getQuery(String mailbox, @Nonnull String query) {
        if (!Strings.isNullOrEmpty(mailbox)) {
            return String.format("{%s}:{%s}", mailbox, query);
        }
        return query;
    }

    @Override
    public Map<String, Integer> getFolderMessageCounts(String parent) throws DataStoreException {
        try {
            LogUtils.info(getClass(),String.format("Getting folder message count for [mailbox= %s]",this.getMailUser()));
            Folder folder = folder(rootFolder, parent);
            if (folder != null) {
                Map<String, Integer> counts = new HashMap<>();
                Folder[] children = findChildFolders(folder, FolderTraversal.Shallow);
                if (children != null && children.length > 0) {
                    for (Folder child : children) {
                        String fname = String.format("%s/%s", parent, child.getDisplayName());
                        LogUtils.info(getClass(), String.format("Checking folder : [%s]", fname));
                        Map<String, Integer> cc = getFolderMessageCounts(fname);
                        if (cc != null && !cc.isEmpty()) {
                            counts.putAll(cc);
                        }
                    }
                }
                int mc = folder.getTotalCount();
                if (mc > 0) {
                    LogUtils.info(getClass(), String.format("Folder message count : [%s][%d]", parent, mc));
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
            Folder folder = folder(parent, false);
            if (folder != null) {
                LogUtils.warn(getClass(), String.format("Cleaning folder [%s]...", parent));
                Folder[] children = findChildFolders(folder, FolderTraversal.Shallow);
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
        try {
            Folder folder = folder(rootFolder, folderPath);
            if (folder != null) {
                Folder[] children = findChildFolders(folder, FolderTraversal.Shallow);
                if (children != null && children.length > 0) {
                    LogUtils.info(getClass(),String.format("sub folders found for folder: %s",folder.getDisplayName()));
                    for (Folder child : children) {
                        String fname = String.format("%s/%s", folderPath, child.getDisplayName());
                        LogUtils.info(getClass(), String.format("checking folder : [%s]", fname));
                        cleanUpEmptyFoldersBasedOnTimePeriod(processingFolderPath,fname,timePeriod);
                        LogUtils.info(getClass(), String.format("checking completed for folder : [%s]", fname));
                    }
                    //all children are processed now
                    LogUtils.info(getClass(), String.format("All subfolders checked for [%s]",folderPath));
                    Folder[] childrenAfterProcessing = findChildFolders(folder, FolderTraversal.Shallow);
                    if(childrenAfterProcessing==null || childrenAfterProcessing.length==0) {
                        LogUtils.info(getClass(),String.format("The folder [%s] does not lead to any emails, hence will be deleted",folderPath));
                        //would have already been deleted
                        if(!withinTimePeriod(timePeriod,folderPath))  checkAndDelete(folder);
                    }
                } else {
                        //this is a node that has no children hence only possibility is an hh folder
                        if(!withinTimePeriod(timePeriod,folderPath))  checkAndDelete(folder);
                }
            }
        } catch (Exception e) {
            throw new DataStoreException(e);
        }
    }

    /**
     * Checks if the processingFolderPath given conforms to the time period restriction from the current time.
     * @param timePeriod in hours
     * @param folderPath This is the folder path that is being checked. It should be of the format <processing_folder_path>/yyyy/MM/dd/hh
     * @return
     */
    private boolean withinTimePeriod(int timePeriod,  String folderPath){
        boolean isWithinTimePeriod=false;
        Pattern p = Pattern.compile("\\d+");//for matching all the integers in the string
        String year="0000",month="00",day="00",hour="00";
        Matcher m = p.matcher(folderPath);
        if(m.find()) year=m.group();
        if(m.find()) month=m.group();
        if(m.find()) day=m.group();
        if(m.find()) hour=m.group();
        String dateString = String.format("%s/%s/%s/%s",year,month,day,hour);
        LogUtils.info(getClass(),String.format("Date information obtained from path %s, [date=%s]",folderPath,dateString));

        try {
            Date folderDate = new SimpleDateFormat("yyyy/MM/dd/hh").parse(dateString);
            Date currentDate = new Date();
            long milliDiff = (currentDate.getTime()-folderDate.getTime());
            int timeDiffInHours =(int) (milliDiff/1000/60/60);
            LogUtils.info(getClass(),String.format("[timePeriod limit=%d hours] [detected time difference = %d hours]",timePeriod,timeDiffInHours));
            if(timeDiffInHours<=timePeriod) isWithinTimePeriod = true;
        } catch (ParseException e) {
            LogUtils.info(getClass(),"Parsing exception");
        }
        return isWithinTimePeriod;
    }


    private void checkAndDelete(Folder folder) throws ServiceLocalException, DataStoreException {
        LogUtils.warn(getClass(), String.format("Cleaning folder [%s]...", folder.getDisplayName()));
        Folder[] children = findChildFolders(folder, FolderTraversal.Shallow);
        if (children != null && children.length > 0) {
            for (Folder f : children) {
                checkAndDelete(f);
            }
        }
        if (isEmpty(folder)) {
            LogUtils.info(getClass(), String.format("Deleting empty folder. [folder=%s]", folder.getDisplayName()));
            if (!deleteFolder(folder)) {
                LogUtils.error(getClass(),
                        String.format("Failed to delete folder. [folder=%s]", folder.getDisplayName()));
            }
        } else {
            LogUtils.info(getClass(), String.format("Folder is not empty. [folder=%s]", folder.getDisplayName()));
        }
    }
}
