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

package io.zyient.base.core;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.micrometer.common.util.internal.logging.Slf4JLoggerFactory;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import io.prometheus.client.hotspot.DefaultExports;
import io.zyient.base.common.AbstractEnvState;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.InvalidDataError;
import io.zyient.base.common.threads.ManagedThread;
import io.zyient.base.common.threads.ThreadManager;
import io.zyient.base.common.utils.NetUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.connections.ConnectionManager;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.env.BaseEnvSettings;
import io.zyient.base.core.errors.Errors;
import io.zyient.base.core.keystore.KeyStore;
import io.zyient.base.core.keystore.settings.KeyStoreSettings;
import io.zyient.base.core.model.ModuleInstance;
import io.zyient.base.core.processing.Processor;
import io.zyient.base.core.services.model.ShutdownStatus;
import io.zyient.base.core.state.BaseStateManager;
import io.zyient.base.core.state.HeartbeatThread;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Accessors(fluent = true)
public abstract class BaseEnv<T extends Enum<?>> implements ThreadManager {
    public static class Constants {
        public static final String __CONFIG_PATH_ENV = "env";
        public static final String CONFIG_ENV_NAME = String.format("%s.name", __CONFIG_PATH_ENV);
        private static final String LOCK_GLOBAL = "global";
        private static final String THREAD_HEARTBEAT = "HEARTBEAT_%s";
    }

    private static MeterRegistry meterRegistry;

    private ConnectionManager connectionManager;
    private String storeKey;
    private KeyStore keyStore;
    private final DistributedLockBuilder dLockBuilder = new DistributedLockBuilder();
    private String environment;
    private HierarchicalConfiguration<ImmutableNode> rootConfig;
    private HierarchicalConfiguration<ImmutableNode> baseConfig;
    private HierarchicalConfiguration<ImmutableNode> managersConfig;
    private List<ExitCallback<T>> exitCallbacks;
    private AbstractEnvState<T> state;
    private ModuleInstance moduleInstance;
    private BaseStateManager stateManager;
    private BaseEnvConfig config;
    private List<InetAddress> hostIPs;
    private final String name;
    private HeartbeatThread heartbeat;
    private BaseEnvSettings settings;
    private String zkBasePath;
    private Map<String, Processor<?, ?>> processors;
    private final Map<String, ManagedThread> managedThreads = new HashMap<>();

    public BaseEnv(@NonNull String name,
                   @NonNull AbstractEnvState<T> state) {
        this.name = name;
        this.state = state;
    }

    public BaseEnv<T> withStoreKey(@NonNull String storeKey) {
        this.storeKey = storeKey;
        return this;
    }

    public BaseEnv<?> withProcessor(@NonNull Processor<?, ?> processor) {
        if (processors == null) {
            processors = new HashMap<>();
        }
        processors.put(processor.name(), processor);
        return this;
    }

    public Processor<?, ?> processor(@NonNull String name) {
        if (processors != null) {
            return processors.get(name);
        }
        return null;
    }

    public BaseEnv<T> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull Class<? extends BaseEnvSettings> type) throws ConfigurationException {
        config = new BaseEnvConfig(xmlConfig, type);
        config.read();
        settings = (BaseEnvSettings) config.settings();
        return setup(xmlConfig, state);
    }

    private BaseEnv<T> setup(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                             @NonNull AbstractEnvState<T> state) throws ConfigurationException {
        try {
            String temp = System.getProperty("java.io.tmpdir");
            temp = String.format("%s/zyient/%s", temp, getClass().getSimpleName());
            File tdir = new File(temp);
            if (!tdir.exists()) {
                if (!tdir.mkdirs()) {
                    throw new IOException(
                            String.format("Error creating temporary folder. [path=%s]", tdir.getAbsolutePath()));
                }
            }

            this.state = state;

            System.setProperty("java.io.tmpdir", temp);
            environment = xmlConfig.getString(Constants.CONFIG_ENV_NAME);
            if (Strings.isNullOrEmpty(environment)) {
                throw new ConfigurationException(
                        String.format("Base Env: missing parameter. [name=%s]", Constants.CONFIG_ENV_NAME));
            }

            rootConfig = xmlConfig;
            baseConfig = config.config();
            if (ConfigReader.checkIfNodeExists(baseConfig, BaseEnvSettings.Constants.__CONFIG_PATH_MANAGERS)) {
                managersConfig = baseConfig.configurationAt(BaseEnvSettings.Constants.__CONFIG_PATH_MANAGERS);
            }
            hostIPs = NetUtils.getInetAddresses();

            moduleInstance = new ModuleInstance()
                    .withIp(NetUtils.getInetAddress(hostIPs))
                    .withStartTime(System.currentTimeMillis());
            moduleInstance.setModule(settings().getModule());
            moduleInstance.setName(settings().getInstance());
            moduleInstance.setInstanceId(moduleInstance.id());

            zkBasePath = settings.getBasePath();

            setup(settings.getModule(), settings.getConnectionConfigPath());

            if (settings.getStateManagerClass() != null) {
                stateManager = settings.getStateManagerClass()
                        .getDeclaredConstructor().newInstance();
                stateManager.withEnvironment(environment(), name)
                        .withModuleInstance(moduleInstance);
                stateManager
                        .init(managersConfig,
                                this);
            }
            if (ConfigReader.checkIfNodeExists(baseConfig, Errors.__CONFIG_PATH)) {
                Errors.create(baseConfig.configurationAt(Errors.__CONFIG_PATH), this);
            }
            DefaultExports.initialize();
            if (meterRegistry == null) {
                LoggingRegistryConfig cfg = new LoggingRegistryConfig() {
                    final Map<String, String> values = Map.of("enabled", "true", "step", "5s");

                    @Override
                    public String get(String s) {
                        return values.get(s);
                    }
                };
                meterRegistry = LoggingMeterRegistry.builder(cfg)
                        .clock(Clock.SYSTEM)
                        .loggingSink(Slf4JLoggerFactory.getInstance(settings.getInstance())::info)
                        .build();
            }
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public ThreadManager addThread(@NonNull String name,
                                   @NonNull ManagedThread thread) {
        managedThreads.put(name, thread);
        return this;
    }

    @Override
    public ManagedThread getThread(@NonNull String name) {
        return managedThreads.get(name);
    }

    @Override
    public ManagedThread removeThread(@NonNull String name) throws Exception {
        return managedThreads.remove(name);
    }

    public void postInit() throws Exception {
        if (settings.isEnableHeartbeat()) {
            String tname = String.format(Constants.THREAD_HEARTBEAT, name);
            heartbeat = new HeartbeatThread(name(), settings.getHeartbeatFreq().normalized())
                    .withStateManager(stateManager);
            ManagedThread heartbeatThread = new ManagedThread(this, heartbeat, name);
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
            throw new InvalidDataError(getClass(), String.format("Key not found: [key=%s]", key));
        }
        if (v.compareTo(value) != 0) {
            throw new SecurityException(String.format("Invalid secret: [key=%s]", key));
        }
    }

    @SuppressWarnings("unchecked")
    public void setup(@NonNull String module,
                      String connectionsConfigPath) throws ConfigurationException {
        try {

            if (ConfigReader.checkIfNodeExists(baseConfig, KeyStoreSettings.__CONFIG_PATH)) {
                String c = baseConfig.getString(KeyStoreSettings.CONFIG_KEYSTORE_CLASS);
                if (Strings.isNullOrEmpty(c)) {
                    throw new ConfigurationException(
                            String.format("Key Store class not defined. [config=%s]",
                                    KeyStoreSettings.CONFIG_KEYSTORE_CLASS));
                }
                Class<? extends KeyStore> cls = (Class<? extends KeyStore>) Class.forName(c);
                keyStore = cls.getDeclaredConstructor().newInstance();
                keyStore.withPassword(storeKey)
                        .init(baseConfig, this);
            }
            this.storeKey = null;

            if (!Strings.isNullOrEmpty(connectionsConfigPath)) {
                connectionManager = new ConnectionManager()
                        .withKeyStore(keyStore);
                connectionManager.init(baseConfig, this, connectionsConfigPath);
            }

            if (ConfigReader.checkIfNodeExists(baseConfig, DistributedLockBuilder.Constants.CONFIG_LOCKS)) {
                dLockBuilder.init(baseConfig, module, this);
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
        if (processors != null) {
            for (Processor<?, ?> processor : processors.values()) {
                processor.close();
            }
            processors.clear();
        }
        if (!managedThreads.isEmpty()) {
            for (String key : managedThreads.keySet()) {
                ManagedThread t = managedThreads.get(key);
                t.close();
                t.join();
            }
            managedThreads.clear();
        }
        if (stateManager != null) {
            stateManager.close();
        }
        if (connectionManager != null) {
            connectionManager.close();
        }
        if (meterRegistry != null) {
            meterRegistry.close();
        }
        if (exitCallbacks != null && !exitCallbacks.isEmpty()) {
            for (ExitCallback<T> callback : exitCallbacks) {
                callback.call(state);
            }
        }
        dLockBuilder.close();
    }

    protected abstract BaseEnv<?> create(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException;

    public String module() {
        return moduleInstance.getModule();
    }

    public String instance() {
        return moduleInstance.getName();
    }

    private static final Map<String, BaseEnv<?>> __instances = new LinkedHashMap<>();
    private static final ReentrantLock __instanceLock = new ReentrantLock();

    @SuppressWarnings("unchecked")
    public static <T extends BaseEnv<?>> T get(@NonNull String name, @NonNull Class<? extends T> type) throws Exception {
        BaseEnv<?> env = __instances.get(name);
        if (env != null) {
            if (!ReflectionHelper.isSuperType(type, env.getClass()))
                throw new Exception(
                        String.format("Invalid Env type. [name=%s][expected=%s][actual=%s]",
                                name, type.getCanonicalName(), env.getClass().getCanonicalName()));
        }
        return (T) env;
    }

    public static void add(@NonNull String name, @NonNull BaseEnv<?> env) {
        __instances.put(name, env);
    }

    public static BaseEnv<?> create(@NonNull Class<? extends BaseEnv<?>> type,
                                    @NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                    String passKey) throws Exception {
        BaseEnv<?> env = type.getDeclaredConstructor()
                .newInstance();
        if (!Strings.isNullOrEmpty(passKey)) {
            env.withStoreKey(passKey);
        }
        env.create(xmlConfig);
        add(env.name, env);
        return env;
    }

    public static boolean remove(@NonNull String name) throws Exception {
        BaseEnv<?> env = __instances.remove(name);
        if (env != null) {
            env.close();
            return true;
        }
        return false;
    }

    public static Map<String, ShutdownStatus> disposeAll() throws Exception {
        __instanceLock.lock();
        try {
            Map<String, ShutdownStatus> statuses = new HashMap<>();
            for (String name : __instances.keySet()) {
                BaseEnv<?> env = __instances.get(name);
                try {
                    env.close();
                    statuses.put(name, new ShutdownStatus(name, true));
                } catch (Throwable t) {
                    statuses.put(name, new ShutdownStatus(name, t));
                }
            }
            __instances.clear();
            return statuses;
        } finally {
            __instanceLock.unlock();
        }
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
                             @NonNull Class<? extends BaseEnvSettings> type) {
            super(config, BaseEnv.Constants.__CONFIG_PATH_ENV, type);
        }

        @Override
        public void read() throws ConfigurationException {
            super.read();
            try {
                BaseEnvSettings settings = (BaseEnvSettings) settings();
                if (checkIfNodeExists(get(), BaseEnvSettings.Constants.__CONFIG_PATH_MANAGERS)) {
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
