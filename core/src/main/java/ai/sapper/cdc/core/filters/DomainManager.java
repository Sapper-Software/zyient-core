package ai.sapper.cdc.core.filters;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.common.filters.DomainFilter;
import ai.sapper.cdc.common.filters.DomainFilterMatcher;
import ai.sapper.cdc.common.filters.DomainFilters;
import ai.sapper.cdc.common.filters.Filter;
import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseStateManager;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public class DomainManager {
    private static final String CONFIG_PATH = "domain";

    private static final String IGNORE_REGEX = "(.*)\\.(_*)COPYING(_*)|/tmp/(.*)|(.*)\\.hive-staging(.*)";
    private ZookeeperConnection zkConnection;
    private HdfsConnection hdfsConnection;
    private String environment;
    private DomainManagerSettings settings;
    private Map<String, DomainFilterMatcher> matchers = new HashMap<>();
    private final List<FilterAddCallback> callbacks = new ArrayList<>();
    private Pattern ignorePattern = Pattern.compile(IGNORE_REGEX);

    public DomainManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull ConnectionManager manger,
                              @NonNull String environment) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(xmlConfig,
                    DomainManagerSettings.__CONFIG_PATH,
                    DomainManagerSettings.class);
            reader.read();
            settings = (DomainManagerSettings) reader.settings();

            this.environment = environment;

            zkConnection = manger.getConnection(settings.zkConnection(), ZookeeperConnection.class);
            if (zkConnection == null) {
                throw new ConfigurationException(
                        String.format("ZooKeeper connection not found. [name=%s]", settings.zkConnection()));
            }
            if (!zkConnection.isConnected()) zkConnection.connect();

            if (!Strings.isNullOrEmpty(settings.hdfsConnection)) {
                hdfsConnection = manger.getConnection(settings.hdfsConnection, HdfsConnection.class);
                if (hdfsConnection == null) {
                    throw new ConfigurationException(
                            String.format("HDFS Connection not found. [name=%s]", settings.hdfsConnection));
                }
                if (!hdfsConnection.isConnected()) {
                    hdfsConnection.connect();
                }
            }
            if (!Strings.isNullOrEmpty(settings.ignoreRegex)) {
                ignorePattern = Pattern.compile(settings.ignoreRegex);
            }
            String path = getZkPath();
            CuratorFramework client = zkConnection.client();
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentContainersIfNeeded().forPath(path);
            }
            readFilters(true);

            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public DomainManager withFilterAddCallback(@NonNull FilterAddCallback callback) {
        callbacks.add(callback);
        return this;
    }

    private void readFilters(boolean useCallbacks) throws Exception {
        matchers.clear();

        String path = getZkPath();
        CuratorFramework client = zkConnection.client();

        if (client.checkExists().forPath(path) != null) {
            List<String> paths = client.getChildren().forPath(path);
            if (paths != null && !paths.isEmpty()) {
                for (String p : paths) {
                    String dp = getZkPath(p);
                    byte[] data = client.getData().forPath(dp);
                    if (data != null && data.length > 0) {
                        String json = new String(data, StandardCharsets.UTF_8);
                        DomainFilters df = JSONUtils.read(json, DomainFilters.class);
                        DomainFilterMatcher m = new DomainFilterMatcher(df.getDomain(), df)
                                .withIgnoreRegex(ignorePattern);
                        matchers.put(df.getDomain(), m);
                        if (useCallbacks && !callbacks.isEmpty()) {
                            for (FilterAddCallback callback : callbacks) {
                                callback.onStart(m);
                            }
                        }
                    }
                }
            }
        }
    }

    public synchronized void refresh() throws Exception {
        readFilters(false);
    }

    private String getZkPath() {
        return new PathUtils.ZkPathBuilder(settings.basePath())
                .withPath(environment)
                .withPath(CONFIG_PATH)
                .withPath(settings.module)
                .build();
    }

    private String getZkPath(String domain) {
        return new PathUtils.ZkPathBuilder(getZkPath())
                .withPath(domain)
                .build();
    }

    public SchemaEntity matches(@NonNull String path) {
        Preconditions.checkNotNull(zkConnection);
        Preconditions.checkState(zkConnection.isConnected());

        if (matchers != null && !matchers.isEmpty()) {
            Map<String, DomainFilterMatcher> ms = matchers;
            for (String d : matchers.keySet()) {
                DomainFilterMatcher m = ms.get(d);
                DomainFilterMatcher.PathFilter pf = m.matches(path);
                if (pf != null) {
                    SchemaEntity dd = new SchemaEntity();
                    dd.setDomain(m.filters().getDomain());
                    dd.setEntity(pf.filter().getEntity());
                    return dd;
                }
            }
        }
        return null;
    }

    public DomainFilter updateGroup(@NonNull String domain,
                                    @NonNull String entity,
                                    @NonNull String group) {
        if (matchers.containsKey(domain)) {
            DomainFilterMatcher matcher = matchers.get(domain);
            return matcher.updateGroup(entity, group);
        }
        return null;
    }

    public DomainFilters add(@NonNull String domain,
                             @NonNull String entity,
                             @NonNull String path,
                             @NonNull String regex,
                             String group) throws Exception {
        Preconditions.checkNotNull(zkConnection);
        Preconditions.checkState(zkConnection.isConnected());

        DomainFilterMatcher matcher = null;
        DomainFilterMatcher.PathFilter filter = null;
        if (!matchers.containsKey(domain)) {
            DomainFilters df = new DomainFilters();
            df.setDomain(domain);
            Filter f = df.add(entity, path, regex, group);

            matcher = new DomainFilterMatcher(domain, df)
                    .withIgnoreRegex(ignorePattern);
            matchers.put(domain, matcher);
            filter = matcher.find(f);
        } else {
            matcher = matchers.get(domain);
            filter = matcher.add(entity, path, regex, group);
        }

        CuratorFramework client = zkConnection.client();
        String json = JSONUtils.asString(matcher.filters(), DomainFilters.class);
        String zp = getZkPath(domain);
        if (client.checkExists().forPath(zp) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(zp);
        }
        Stat stat = client.setData().forPath(zp, json.getBytes(StandardCharsets.UTF_8));
        DefaultLogger.debug(String.format("Added Domain Filter: [path=%s][filter=%s]", zp, json));
        if (!callbacks.isEmpty()) {
            for (FilterAddCallback callback : callbacks) {
                callback.process(matcher, filter, path);
            }
        }
        return matcher.filters();
    }

    public DomainFilter remove(@NonNull String domain,
                               @NonNull String entity) throws Exception {
        Preconditions.checkNotNull(zkConnection);
        Preconditions.checkState(zkConnection.isConnected());

        if (matchers.containsKey(domain)) {
            DomainFilterMatcher matcher = matchers.get(domain);
            DomainFilter df = matcher.remove(entity);
            if (df != null) {
                saveDomainFilters(matcher.filters());
            }
            return df;
        }
        return null;
    }

    public List<Filter> remove(@NonNull String domain,
                               @NonNull String entity,
                               @NonNull String path) throws Exception {
        Preconditions.checkNotNull(zkConnection);
        Preconditions.checkState(zkConnection.isConnected());

        if (matchers.containsKey(domain)) {
            DomainFilterMatcher matcher = matchers.get(domain);
            List<Filter> fs = matcher.remove(entity, path);
            if (fs != null && !fs.isEmpty()) {
                saveDomainFilters(matcher.filters());
            }
            return fs;
        }
        return null;
    }

    public Filter remove(@NonNull String domain,
                         @NonNull String entity,
                         @NonNull String path,
                         @NonNull String regex) throws Exception {
        Preconditions.checkNotNull(zkConnection);
        Preconditions.checkState(zkConnection.isConnected());

        if (matchers.containsKey(domain)) {
            DomainFilterMatcher matcher = matchers.get(domain);
            Filter fs = matcher.remove(entity, path, regex);
            if (fs != null) {
                saveDomainFilters(matcher.filters());
            }
            return fs;
        }
        return null;
    }

    private void saveDomainFilters(DomainFilters filters) throws Exception {
        CuratorFramework client = zkConnection.client();
        String json = JSONUtils.asString(filters, DomainFilters.class);
        String zp = getZkPath(filters.getDomain());
        if (client.checkExists().forPath(zp) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(zp);
        }
        Stat stat = client.setData().forPath(zp, json.getBytes(StandardCharsets.UTF_8));
        DefaultLogger.debug(String.format("Removed Domain Filter: [path=%s][filter=%s]", zp, json));
    }

    @Getter
    @Setter
    public static class DomainManagerSettings extends BaseStateManager.BaseStateManagerSettings {
        public static final class Constants {
            public static final String CONFIG_HDFS_CONNECTION = "hdfs";
            public static final String CONFIG_IGNORE_REGEX = "ignoreRegex";
            public static final String CONFIG_MODULE = "module";
        }

        private static final String __CONFIG_PATH = "managers.domain";

        @Config(name = Constants.CONFIG_HDFS_CONNECTION, required = false)
        private String hdfsConnection;
        @Config(name = Constants.CONFIG_IGNORE_REGEX, required = false)
        private String ignoreRegex;
        @Config(name = Constants.CONFIG_MODULE)
        private String module;

        public DomainManagerSettings() {
        }

        public DomainManagerSettings(@NonNull Settings source) {
            super(source);
            Preconditions.checkArgument(source instanceof DomainManagerSettings);
            this.hdfsConnection = ((DomainManagerSettings) source).hdfsConnection;
            this.ignoreRegex = ((DomainManagerSettings) source).ignoreRegex;
            this.module = ((DomainManagerSettings) source).module;
        }
    }
}
