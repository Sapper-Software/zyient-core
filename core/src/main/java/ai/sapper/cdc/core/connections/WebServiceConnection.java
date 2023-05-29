package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.config.ZkConfigReader;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.EConnectionType;
import ai.sapper.cdc.core.connections.settngs.WebServiceConnectionSettings;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.JerseyWebTarget;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

@Getter
@Accessors(fluent = true)
public class WebServiceConnection implements Connection {
    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();
    private URL endpoint;
    private WebServiceConnectionConfig config;
    private JerseyClient client;
    private String name;
    private WebServiceConnectionSettings settings;

    public WebServiceConnection() {
    }

    public WebServiceConnection(@NonNull String name,
                                @NonNull String endpoint) throws MalformedURLException {
        this.name = name;
        this.endpoint = new URL(endpoint);
        client = new JerseyClientBuilder().build();
        state.setState(EConnectionState.Initialized);
    }

    /**
     * @return
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            try {
                config = new WebServiceConnectionConfig(xmlConfig);
                config.read();
                settings = (WebServiceConnectionSettings) config.settings();
                client = new JerseyClientBuilder().build();
                name = settings.getName();
                endpoint = new URL(settings.getEndpoint());

                state.setState(EConnectionState.Initialized);
                return this;
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
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear();

                CuratorFramework client = connection.client();
                String zkPath = new PathUtils.ZkPathBuilder(path)
                        .withPath(WebServiceConnectionConfig.__CONFIG_PATH)
                        .build();
                ZkConfigReader reader = new ZkConfigReader(client, WebServiceConnectionSettings.class);
                if (!reader.read(zkPath)) {
                    throw new ConnectionError(
                            String.format("WebService Connection settings not found. [path=%s]", zkPath));
                }
                settings = (WebServiceConnectionSettings) reader.settings();
                settings.validate();

                this.client = new JerseyClientBuilder().build();
                this.name = settings.getName();
                endpoint = new URL(settings.getEndpoint());

                state.setState(EConnectionState.Initialized);
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
        return this;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof WebServiceConnectionSettings);
        synchronized (state) {
            try {
                this.settings = (WebServiceConnectionSettings) settings;
                client = new JerseyClientBuilder().build();
                name = this.settings.getName();
                endpoint = new URL(this.settings.getEndpoint());

                state.setState(EConnectionState.Initialized);
                return this;
            } catch (Exception ex) {
                state.error(ex);
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection connect() throws ConnectionError {
        throw new ConnectionError("Method should not be called...");
    }

    public JerseyWebTarget connect(@NonNull String path) throws ConnectionError {
        try {
            return client.target(endpoint.toURI()).path(path);
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
    }

    /**
     * @return
     */
    @Override
    public Throwable error() {
        return state.getError();
    }

    /**
     * @return
     */
    @Override
    public EConnectionState connectionState() {
        return state.getState();
    }

    /**
     * @return
     */
    @Override
    public boolean isConnected() {
        return state.getState() == EConnectionState.Initialized;
    }

    @Override
    public String path() {
        return WebServiceConnectionConfig.__CONFIG_PATH;
    }

    @Override
    public EConnectionType type() {
        return settings.getType();
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

    }


    @Getter
    @Accessors(fluent = true)
    public static class WebServiceConnectionConfig extends ConnectionConfig {
        private static final String __CONFIG_PATH = "rest";

        public WebServiceConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, WebServiceConnectionSettings.class);
        }
    }
}
