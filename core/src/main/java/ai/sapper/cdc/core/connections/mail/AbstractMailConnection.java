package ai.sapper.cdc.core.connections.mail;

import ai.sapper.cdc.common.ICloseDelegate;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.ZkConfigReader;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.settings.EConnectionType;
import ai.sapper.cdc.core.connections.settings.mail.ExchangeConnectionSettings;
import ai.sapper.cdc.core.connections.settings.mail.MailConnectionSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

@Getter
@Accessors(fluent = true)
public abstract class AbstractMailConnection<T> implements Connection, ICloseDelegate<T> {
    private final Class<? extends MailConnectionSettings> settingsType;
    private final String path;
    protected MailConnectionSettings settings;
    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();
    protected BaseEnv<?> env;
    protected MailConfigReader configReader;

    protected AbstractMailConnection(@NonNull Class<? extends MailConnectionSettings> settingsType,
                                     String path) {
        this.settingsType = settingsType;
        this.path = path;
    }

    @Override
    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            if (state.isConnected()) return this;
            try {
                this.env = env;
                if (Strings.isNullOrEmpty(path)) {
                    configReader = new MailConfigReader(config, settingsType);
                } else {
                    configReader = new MailConfigReader(config, path, settingsType);
                }
                configReader.read();
                settings = (MailConnectionSettings) configReader.settings();
                return setup(settings, env);
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            if (state.isConnected()) return this;
            try {
                this.env = env;
                String p = this.path;
                if (Strings.isNullOrEmpty(p)) {
                    p = MailConfigReader.__CONFIG_PATH;
                }
                CuratorFramework client = connection.client();
                String zkPath = new PathUtils.ZkPathBuilder(path)
                        .withPath(p)
                        .build();
                ZkConfigReader reader = new ZkConfigReader(client, settingsType);
                if (!reader.read(zkPath)) {
                    throw new ConnectionError(
                            String.format("Exchange Connection settings not found. [path=%s]", zkPath));
                }
                settings = (MailConnectionSettings) reader.settings();
                return setup(settings, env);
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    @Override
    public Throwable error() {
        return state.getError();
    }

    @Override
    public EConnectionState connectionState() {
        return state.getState();
    }

    @Override
    public boolean isConnected() {
        return state.isConnected();
    }


    @Override
    public EConnectionType type() {
        return EConnectionType.email;
    }

    public abstract T connection() throws ConnectionError;

    public abstract boolean hasTransactionSupport();

    public static class MailConfigReader extends ConfigReader {
        public static final String __CONFIG_PATH = "mail";

        public MailConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                @NonNull String path,
                                @NonNull Class<? extends MailConnectionSettings> type) {
            super(config, path, type);
        }

        public MailConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                @NonNull Class<? extends MailConnectionSettings> type) {
            super(config, __CONFIG_PATH, type);
        }

    }
}
