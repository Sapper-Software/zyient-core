package ai.sapper.cdc.core.connections.mail;

import com.codekutter.common.model.ConnectionConfig;
import com.codekutter.common.stores.ConnectionException;
import com.codekutter.common.stores.EConfigSource;
import com.codekutter.common.stores.EConnectionState;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.common.utils.ThreadCache;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.codekutter.zconfig.common.model.nodes.ConfigValueNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.ingestion.common.model.EMailServerType;
import com.ingestion.common.model.MailConnectionDbConfig;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

@Getter
@Setter
@Accessors(fluent = true)
public class IMAPConnection extends AbstractMailConnection<Store> {
    private static final String CONFIG_MAIL_PROTOCOL_PARAM = "mail.store.protocol";
    private static final String CONFIG_SOCKET_FACTORY_PARAM = "mail.imap.socketFactory.class";
    private static final String CONFIG_SOCKET_FALLBACK_PARAM = "mail.imap.socketFactory.fallback";
    private static final String CONFIG_SOCKET_PORT_PARAM = "mail.imap.socketFactory.port";
    private static final String CONFIG_IMAP_PORT_PARAM = "mail.imap.port";
    private static final String CONFIG_IMAP_HOST_PARAM = "mail.imap.host";
    private static final String CONFIG_IMAP_USER_PARAM = "mail.imap.user";
    private static final String CONFIG_IMAPS_PORT_PARAM = "mail.imaps.port";
    private static final String CONFIG_IMAPS_HOST_PARAM = "mail.imaps.host";
    private static final String CONFIG_IMAPS_USER_PARAM = "mail.imaps.user";
    private static final String CONFIG_IMAP_SSL_PARAM = "mail.imap.ssl.enable";
    private static final String CONFIG_ENCRYPTION_TYPE = "mail.imap.starttls.enable";
    public static final String CONFIG_MAIL_PROTOCOL_SSL = "imaps";
    public static final String CONFIG_MAIL_PROTOCOL_NOSSL = "imap";

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private ThreadCache<Store> threadCache = new ThreadCache<>();

    public void open() throws ConfigurationException {
        try {
            Session readSession = setup();
            Store store = readSession.getStore();
            LogUtils.warn(getClass(), String.format(" [store=%s]", store));
            store.connect(username, password.getDecryptedValue());
            LogUtils.warn(getClass(), String.format(" store.connect"));
            threadCache.put(store);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        try {
            Preconditions.checkArgument(node instanceof ConfigPathNode);

            ConfigurationAnnotationProcessor.readConfigAnnotations(getClass(), (ConfigPathNode) node, this);
            if (configSource() == EConfigSource.File) {
                if (Strings.isNullOrEmpty(mailServer))
                    throw ConfigurationException.propertyNotFoundException("MAIL SERVER");
                if (port <= 0)
                    throw ConfigurationException.propertyNotFoundException("MAIL SERVER PORT");
                if (Strings.isNullOrEmpty(username))
                    throw ConfigurationException.propertyNotFoundException("USER NAME");
                if (password == null)
                    throw ConfigurationException.propertyNotFoundException("USER PASSWORD");
                if (((ConfigPathNode) node).parmeters() != null
                        && !((ConfigPathNode) node).parmeters().isEmpty()) {
                    Map<String, ConfigValueNode> params = ((ConfigPathNode) node).parmeters().getKeyValues();
                    for (String key : params.keySet()) {
                        ConfigValueNode vn = params.get(key);
                        options.put(key, vn.getValue());
                        LogUtils.debug(getClass(), String.format("Added option: {key=%s][value=%s]", key, vn.getValue()));
                    }
                }
            } else {
                throw new ConfigurationException(
                        String.format("Method shouldn't be called for source type. [source type=%s]",
                                configSource().name()));
            }
            setup();
            state().setState(EConnectionState.Open);
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            state().setError(t);
            throw new ConfigurationException(t);
        }
    }

    private Session setup() throws ConfigurationException {
        Properties props = new Properties();

        if (useSSL) {
            props.setProperty(CONFIG_IMAPS_HOST_PARAM, mailServer);
            props.setProperty(CONFIG_IMAPS_PORT_PARAM, String.valueOf(port));
            props.setProperty(CONFIG_MAIL_PROTOCOL_PARAM, CONFIG_MAIL_PROTOCOL_SSL);

            props.setProperty(CONFIG_IMAP_SSL_PARAM, String.valueOf(true));
            props.setProperty(CONFIG_SOCKET_FACTORY_PARAM,
                    javax.net.ssl.SSLSocketFactory.class.getCanonicalName());
            props.setProperty(CONFIG_SOCKET_FALLBACK_PARAM, String.valueOf(false));
            props.setProperty(CONFIG_SOCKET_PORT_PARAM, String.valueOf(port));
            props.setProperty(CONFIG_IMAPS_USER_PARAM, username);
        } else {
            props.setProperty(CONFIG_MAIL_PROTOCOL_PARAM, CONFIG_MAIL_PROTOCOL_NOSSL);
            props.setProperty(CONFIG_IMAP_HOST_PARAM, mailServer);
            props.setProperty(CONFIG_IMAP_PORT_PARAM, String.valueOf(port));
            props.setProperty(CONFIG_IMAP_USER_PARAM, username);
        }
        if (options != null && !options.isEmpty()) {
            props.putAll(options);
        }
        return Session.getInstance(props);
    }

    @Override
    public void configure(ConnectionConfig config) throws ConfigurationException {
        Preconditions.checkArgument(config instanceof MailConnectionDbConfig);
        MailConnectionDbConfig mdc = (MailConnectionDbConfig) config;
        Preconditions.checkArgument(mdc.getType() == EMailServerType.IMAP);
        super.configure(config);
        state().setState(EConnectionState.Open);
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        try {
            if (state().getState() == EConnectionState.Open) {
                LogUtils.info(getClass(), String.format("Closing Data Source : [name=%s]", name()));
                state().setState(EConnectionState.Closed);
            } else if (state().hasError()) {
                LogUtils.warn(getClass(), String.format("Data Source in error state. " +
                        "[name=%s][error=%s]", name(), state().getError().getLocalizedMessage()));
            }
            threadCache.close(store -> {
                if (store != null && store.isConnected()) store.close();
            });
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public Store connection() {
        try {
            state().checkOpened();
            if (!threadCache.contains()) {
                open();
            }
            Store store = threadCache.get();
            if (!store.isConnected()) {
                store.connect(username, password.getDecryptedValue());
            }
            return store;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public boolean hasTransactionSupport() {
        return false;
    }

    @Override
    public void close(@Nonnull Store store) throws ConnectionException {
        try {
            close(Thread.currentThread().getId());
        } catch (MessagingException e) {
            throw new ConnectionException(e, getClass());
        }
    }

    public void close(long threadId) throws MessagingException {
        if (threadCache.contains()) {
            Store store = threadCache.get();
            if (store != null && store.isConnected()) store.close();
            threadCache.remove();
        }
    }
}
