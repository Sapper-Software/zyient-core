package ai.sapper.cdc.core.connections.hadoop;

import ai.sapper.cdc.common.config.ZkConfigReader;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.settings.ConnectionSettings;
import ai.sapper.cdc.core.connections.settings.HdfsConnectionSettings;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

import java.net.URI;

@Getter
@Accessors(fluent = true)
public class HdfsHAConnection extends HdfsConnection {
    public static class Constants {
        public static final String DFS_NAME_SERVICES = "dfs.nameservices";
        public static final String DFS_FAILOVER_PROVIDER = "dfs.client.failover.proxy.provider.%s";
        public static final String DFS_NAME_NODES = "dfs.ha.namenodes.%s";
        public static final String DFS_NAME_NODE_ADDRESS = "dfs.namenode.rpc-address.%s.%s";
    }

    private HdfsHAConfig config;

    /**
     * @return
     */
    @Override
    public String name() {
        return settings.getName();
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
                if (state.isConnected()) {
                    close();
                }
                state.clear();
                config = new HdfsHAConfig(xmlConfig);
                config.read();
                settings = (HdfsConnectionSettings.HdfsBaseSettings) config.settings();

                setupHadoopConfig();

                state.setState(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
            }
        }
        return this;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof HdfsConnectionSettings.HdfsHASettings);
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear();
                this.settings = (HdfsConnectionSettings.HdfsBaseSettings) settings;

                setupHadoopConfig();

                state.setState(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
            }
        }
        return this;
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
                        .withPath(HdfsHAConfig.__CONFIG_PATH)
                        .build();
                ZkConfigReader reader = new ZkConfigReader(client, HdfsConnectionSettings.HdfsHASettings.class);
                if (!reader.read(zkPath)) {
                    throw new ConnectionError(
                            String.format("HDFS(HA) Connection settings not found. [path=%s]", zkPath));
                }
                settings = (HdfsConnectionSettings.HdfsBaseSettings) reader.settings();
                settings.validate();

                setupHadoopConfig();

                state.setState(EConnectionState.Initialized);
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
        return this;
    }

    private void setupHadoopConfig() throws Exception {
        Preconditions.checkState(settings instanceof HdfsConnectionSettings.HdfsHASettings);
        hdfsConfig = new Configuration();
        hdfsConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getCanonicalName());
        hdfsConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getCanonicalName());
        hdfsConfig.set(Constants.DFS_NAME_SERVICES, ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService());
        hdfsConfig.set(String.format(Constants.DFS_FAILOVER_PROVIDER,
                        ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService()),
                ((HdfsConnectionSettings.HdfsHASettings) settings).getFailoverProvider());
        String nns = String.format("%s,%s",
                ((HdfsConnectionSettings.HdfsHASettings) settings).getNameNodeAddresses()[0][0],
                ((HdfsConnectionSettings.HdfsHASettings) settings).getNameNodeAddresses()[1][0]);
        hdfsConfig.set(String.format(Constants.DFS_NAME_NODES,
                ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService()), nns);
        for (String[] nn : ((HdfsConnectionSettings.HdfsHASettings) settings).getNameNodeAddresses()) {
            hdfsConfig.set(String.format(Constants.DFS_NAME_NODE_ADDRESS,
                    ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService(), nn[0]), nn[1]);
        }
        if (settings.isSecurityEnabled()) {
            enableSecurity(hdfsConfig);
        }
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection connect() throws ConnectionError {
        Preconditions.checkState(settings instanceof HdfsConnectionSettings.HdfsHASettings);
        synchronized (state) {
            if (!state.isConnected()
                    && (state.getState() == EConnectionState.Initialized
                    || state.getState() == EConnectionState.Closed)) {
                state.clear();
                try {
                    fileSystem = FileSystem.get(URI.create(String.format("hdfs://%s",
                            ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService())), hdfsConfig);
                    if (settings.isAdminEnabled()) {
                        adminClient = new HdfsAdmin(URI.create(String.format("hdfs://%s",
                                ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService())), hdfsConfig);
                    }
                    if (settings.getParameters() != null && !settings.getParameters().isEmpty()) {
                        for (String key : settings.getParameters().keySet()) {
                            hdfsConfig.set(key, settings.getParameters().get(key));
                        }
                    }
                    dfsClient = new DFSClient(URI.create(String.format("hdfs://%s",
                            ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService())), hdfsConfig);

                    state.setState(EConnectionState.Connected);
                } catch (Throwable t) {
                    state.error(t);
                    throw new ConnectionError("Error opening HDFS connection.", t);
                }
            }
        }
        return this;
    }

    @Override
    public String path() {
        return HdfsHAConfig.__CONFIG_PATH;
    }


    @Getter
    @Accessors(fluent = true)
    public static final class HdfsHAConfig extends HdfsConfig {
        private static final String __CONFIG_PATH = "hdfs-ha";


        public HdfsHAConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config,
                    __CONFIG_PATH,
                    HdfsConnectionSettings.HdfsHASettings.class,
                    HdfsHAConnection.class);
        }
    }
}
