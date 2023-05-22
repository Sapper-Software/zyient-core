package ai.sapper.cdc.core;

import ai.sapper.cdc.common.AbstractEnvState;
import ai.sapper.cdc.common.audit.AuditLogger;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.NetUtils;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.io.FileSystemManager;
import ai.sapper.cdc.core.keystore.KeyStore;
import ai.sapper.cdc.core.model.ModuleInstance;
import ai.sapper.cdc.core.state.BaseStateManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.MeterRegistry;
import io.prometheus.client.hotspot.DefaultExports;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import javax.ws.rs.NotFoundException;
import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Accessors(fluent = true)
public abstract class BaseEnv<T extends Enum<?>> {
    public static class Constants {
        public static final String __CONFIG_PATH_ENV = "env";
        public static final String CONFIG_ENV_NAME = String.format("%s.name", __CONFIG_PATH_ENV);
        private static final String LOCK_GLOBAL = "global";
    }

    private static MeterRegistry meterRegistry;

    private ConnectionManager connectionManager;
    private String storeKey;
    private KeyStore keyStore;
    private final DistributedLockBuilder dLockBuilder = new DistributedLockBuilder();
    private String environment;
    private HierarchicalConfiguration<ImmutableNode> rootConfig;
    private HierarchicalConfiguration<ImmutableNode> baseConfig;
    private List<ExitCallback<T>> exitCallbacks;
    private AbstractEnvState<T> state;
    private ModuleInstance moduleInstance;
    private BaseStateManager stateManager;
    private AuditLogger auditLogger;
    private BaseEnvConfig config;
    private List<InetAddress> hostIPs;
    private final String name;
    private Thread heartbeatThread;
    private HeartbeatThread heartbeat;
    private BaseEnvSettings settings;
    private FileSystemManager fileSystemManager;

    public BaseEnv(@NonNull String name) {
        this.name = name;
    }

    public BaseEnv<T> withStoreKey(@NonNull String storeKey) {
        this.storeKey = storeKey;
        return this;
    }

    public BaseEnv<T> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull BaseEnvConfig config,
                           @NonNull AbstractEnvState<T> state) throws ConfigurationException {
        try {
            this.config = config;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
        return setup(xmlConfig, state);
    }

    public BaseEnv<T> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull AbstractEnvState<T> state,
                           @NonNull Class<? extends BaseEnvSettings> type) throws ConfigurationException {
        config = new BaseEnvConfig(xmlConfig, type);
        config.read();
        settings = (BaseEnvSettings) config.settings();
        return setup(xmlConfig, state);
    }

    @SuppressWarnings("unchecked")
    private BaseEnv<T> setup(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                             @NonNull AbstractEnvState<T> state) throws ConfigurationException {
        try {
            String temp = System.getProperty("java.io.tmpdir");
            temp = String.format("%s/sapper/cdc/%s", temp, getClass().getSimpleName());
            File tdir = new File(temp);
            if (!tdir.exists()) {
                tdir.mkdirs();
            }
            System.setProperty("java.io.tmpdir", temp);
            environment = xmlConfig.getString(Constants.CONFIG_ENV_NAME);
            if (Strings.isNullOrEmpty(environment)) {
                throw new ConfigurationException(
                        String.format("Base Env: missing parameter. [name=%s]", Constants.CONFIG_ENV_NAME));
            }

            rootConfig = config.config();
            baseConfig = xmlConfig;

            hostIPs = NetUtils.getInetAddresses();

            moduleInstance = new ModuleInstance()
                    .withIp(NetUtils.getInetAddress(hostIPs))
                    .withStartTime(System.currentTimeMillis());
            moduleInstance.setModule(settings().getModule());
            moduleInstance.setName(settings().getInstance());
            moduleInstance.setInstanceId(moduleInstance.id());

            setup(settings.getModule(), settings.getConnectionConfigPath());

            if (settings.getStateManagerClass() != null) {
                stateManager = settings.getStateManagerClass()
                        .getDeclaredConstructor().newInstance();
                stateManager.withEnvironment(environment(), name)
                        .withModuleInstance(moduleInstance);
                stateManager
                        .init(rootConfig,
                                this);
            }
            if (ConfigReader.checkIfNodeExists(rootConfig,
                    AuditLogger.__CONFIG_PATH)) {
                String c = rootConfig.getString(AuditLogger.CONFIG_AUDIT_CLASS);
                if (Strings.isNullOrEmpty(c)) {
                    throw new ConfigurationException(
                            String.format("Audit Logger class not specified. [node=%s]",
                                    AuditLogger.CONFIG_AUDIT_CLASS));
                }
                Class<? extends AuditLogger> cls = (Class<? extends AuditLogger>) Class.forName(c);
                auditLogger = cls.getDeclaredConstructor().newInstance();
                auditLogger.init(rootConfig);
            }

            if (ConfigReader.checkIfNodeExists(rootConfig, FileSystemManager.__CONFIG_PATH)) {
                fileSystemManager = new FileSystemManager();
                fileSystemManager.init(rootConfig, this);
            }
            DefaultExports.initialize();

            this.state = state;

            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public void postInit() {
        if (settings.isEnableHeartbeat()) {
            heartbeat = new HeartbeatThread(name()).withStateManager(stateManager);
            heartbeatThread = new Thread(heartbeat);
            heartbeatThread.start();
        }
    }

    public DistributedLock globalLock() throws Exception {
        return createLock(Constants.LOCK_GLOBAL);
    }

    public DistributedLock createLock(@NonNull String name) throws Exception {
        return createLock(stateManager.zkPath(), module(), name);
    }

    public void validate(@NonNull String key,
                         @NonNull String value) throws Exception {
        Preconditions.checkNotNull(keyStore);
        String v = keyStore.read(key);
        if (Strings.isNullOrEmpty(v)) {
            throw new NotFoundException(String.format("Key not found: [key=%s]", key));
        }
        if (v.compareTo(value) != 0) {
            throw new SecurityException(String.format("Invalid secret: [key=%s]", key));
        }
    }

    public void setup(@NonNull String module,
                      @NonNull String connectionsConfigPath) throws ConfigurationException {
        try {

            if (ConfigReader.checkIfNodeExists(rootConfig, KeyStore.__CONFIG_PATH)) {
                String c = rootConfig.getString(KeyStore.CONFIG_KEYSTORE_CLASS);
                if (Strings.isNullOrEmpty(c)) {
                    throw new ConfigurationException(
                            String.format("Key Store class not defined. [config=%s]", KeyStore.CONFIG_KEYSTORE_CLASS));
                }
                Class<? extends KeyStore> cls = (Class<? extends KeyStore>) Class.forName(c);
                keyStore = cls.getDeclaredConstructor().newInstance();
                keyStore.withPassword(storeKey)
                        .init(rootConfig);
            }
            this.storeKey = null;

            connectionManager = new ConnectionManager()
                    .withKeyStore(keyStore);
            connectionManager.init(rootConfig, this, connectionsConfigPath);

            if (ConfigReader.checkIfNodeExists(rootConfig, DistributedLockBuilder.Constants.CONFIG_LOCKS)) {
                dLockBuilder.withEnv(environment)
                        .init(rootConfig, module, connectionManager);
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public synchronized BaseEnv<T> addExitCallback(@NonNull ExitCallback<T> callback) {
        if (exitCallbacks == null) {
            exitCallbacks = new ArrayList<>();
        }
        exitCallbacks.add(callback);
        return this;
    }

    public DistributedLock createLock(@NonNull String path,
                                      @NonNull String module,
                                      @NonNull String name) throws Exception {
        Preconditions.checkNotNull(dLockBuilder);
        return dLockBuilder.createLock(path, module, name);
    }

    public DistributedLock createCustomLock(@NonNull String name,
                                            @NonNull String path,
                                            @NonNull ZookeeperConnection connection,
                                            long timeout) throws Exception {
        Preconditions.checkNotNull(dLockBuilder);
        Preconditions.checkArgument(timeout > 0);
        return dLockBuilder.createLock(name, path, connection, timeout);
    }

    public void saveLocks() throws Exception {
        Preconditions.checkNotNull(dLockBuilder);
        dLockBuilder.save();
    }

    public void close() throws Exception {
        if (heartbeatThread != null) {
            heartbeat().terminate();
            heartbeatThread().join();
            heartbeatThread = null;
        }
        if (stateManager != null) {
            stateManager.close();
        }
        if (connectionManager != null) {
            connectionManager.close();
        }
        if (exitCallbacks != null && !exitCallbacks.isEmpty()) {
            for (ExitCallback<T> callback : exitCallbacks) {
                callback.call(state);
            }
        }
        if (dLockBuilder != null) {
            dLockBuilder.close();
        }
    }


    public String module() {
        return moduleInstance.getModule();
    }

    public String instance() {
        return moduleInstance.getName();
    }

    public String source() {
        return settings.getSource();
    }


    private static final Map<String, BaseEnv<?>> __instances = new LinkedHashMap<>();
    private static final ReentrantLock __instanceLock = new ReentrantLock();

    @SuppressWarnings("unchecked")
    public static <T extends BaseEnv<?>> T get(@NonNull String name, @NonNull Class<? extends T> type) throws Exception {
        BaseEnv<?> env = __instances.get(name);
        if (env != null) {
            if (!ReflectionUtils.isSuperType(type, env.getClass()))
                throw new Exception(
                        String.format("Invalid Env type. [name=%s][expected=%s][actual=%s]",
                                name, type.getCanonicalName(), env.getClass().getCanonicalName()));
        }
        return (T) env;
    }

    public static void add(@NonNull String name, @NonNull BaseEnv<?> env) {
        __instances.put(name, env);
    }

    public static BaseEnv<?> remove(@NonNull String name) {
        return __instances.remove(name);
    }

    public static void initLock() {
        __instanceLock.lock();
    }

    public static void initUnLock() {
        __instanceLock.unlock();
    }

    public static MeterRegistry registry() {
        return meterRegistry;
    }

    public static void registry(@NonNull MeterRegistry registry) {
        meterRegistry = registry;
    }


    @Getter
    @Accessors(fluent = true)
    public static class BaseEnvConfig extends ConfigReader {

        public BaseEnvConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                             @NonNull Class<? extends  BaseEnvSettings> type) {
            super(config, BaseEnv.Constants.__CONFIG_PATH_ENV, type);
        }

        @Override
        public void read() throws ConfigurationException {
            super.read();
            try {
                BaseEnvSettings settings = (BaseEnvSettings) settings();
                if (ConfigReader.checkIfNodeExists(get(), BaseEnvSettings.Constants.__CONFIG_PATH_MANAGERS)) {
                    HierarchicalConfiguration<ImmutableNode> node
                            = get().configurationAt(BaseEnvSettings.Constants.__CONFIG_PATH_MANAGERS);
                    if (node != null) {
                        settings.setManagersConfig(node);
                    }
                }
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }
}
