/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.base.core.auditing;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.audit.AuditRecord;
import io.zyient.base.common.audit.EAuditType;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.entity.IKeyed;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.stores.DataStoreManager;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public class AuditManager implements Closeable {
    public static final String __CONFIG_PATH = "audit-manager";

    private static final AuditManager __instance = new AuditManager();
    private final Map<String, AbstractAuditLogger> loggers = new ConcurrentHashMap<>();
    private final Map<Class<? extends IKeyed<?>>, AbstractAuditLogger> entityIndex = new HashMap<>();
    private final Map<Class<? extends IAuditContextGenerator>, IAuditContextGenerator> contextGenerators = new HashMap<>();
    private AbstractAuditLogger defaultLogger = null;
    @Setter(AccessLevel.NONE)
    private DataStoreManager dataStoreManager;
    private BaseEnv<?> env;

    public static void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                            @NonNull BaseEnv<?> env) throws ConfigurationException {
        __instance.configure(xmlConfig, env);
    }

    public static AuditManager get() {
        return __instance;
    }

    public AuditManager withDataStoreManager(@NonNull DataStoreManager dataStoreManager) {
        this.dataStoreManager = dataStoreManager;
        return this;
    }

    public IAuditContextGenerator getContextGenerator(@NonNull Class<? extends IAuditContextGenerator> type) throws AuditException {
        try {
            synchronized (contextGenerators) {
                if (!contextGenerators.containsKey(type)) {
                    IAuditContextGenerator g = type.newInstance();
                    contextGenerators.put(type, g);
                }
                return contextGenerators.get(type);
            }
        } catch (Exception ex) {
            throw new AuditException(ex);
        }
    }

    public <T extends IKeyed<?>> AuditRecord audit(@NonNull Class<?> dataStoreType,
                                                   @NonNull String dataStoreName,
                                                   @NonNull EAuditType type,
                                                   @NonNull T entity,
                                                   String changeDelta,
                                                   String changeContext,
                                                   @NonNull Principal user) throws AuditException {
        AbstractAuditLogger logger = getLogger(entity.getClass());
        if (logger != null) {
            return logger.write(dataStoreType, dataStoreName, type, entity, entity.getClass(), changeDelta, changeContext, user);
        }
        return null;
    }

    public <T extends IKeyed<?>> AuditRecord audit(@NonNull Class<?> dataStoreType,
                                                   @NonNull String dataStoreName,
                                                   @NonNull EAuditType type,
                                                   @NonNull T entity,
                                                   String changeDelta,
                                                   String changeContext,
                                                   @NonNull Principal user,
                                                   @NonNull IAuditSerDe serializer) throws AuditException {
        AbstractAuditLogger logger = getLogger(entity.getClass());
        if (logger != null) {
            return logger.write(dataStoreType, dataStoreName, type, entity, entity.getClass(), changeDelta, changeContext, user, serializer);
        }
        return null;
    }

    public <T extends IKeyed<?>> AuditRecord audit(@NonNull Class<?> dataStoreType,
                                                   @NonNull String dataStoreName,
                                                   @NonNull String logger,
                                                   @NonNull EAuditType type,
                                                   @NonNull T entity,
                                                   String changeDelta,
                                                   String changeContext,
                                                   @NonNull Principal user) throws AuditException {
        if (loggers.containsKey(logger)) {
            return loggers.get(logger).write(dataStoreType, dataStoreName, type, entity, entity.getClass(), changeDelta, changeContext, user);
        }
        return null;
    }

    public <T extends IKeyed> AuditRecord audit(@NonNull Class<?> dataStoreType,
                                                @NonNull String dataStoreName,
                                                @NonNull String logger,
                                                @NonNull EAuditType type,
                                                @NonNull T entity,
                                                String changeDelta,
                                                String changeContext,
                                                @NonNull IAuditSerDe serializer,
                                                @NonNull Principal user) throws AuditException {
        if (loggers.containsKey(logger)) {
            return loggers.get(logger).write(dataStoreType, dataStoreName, type, entity, entity.getClass(), changeDelta, changeContext, user, serializer);
        }
        return null;
    }

    public void flush() throws AuditException {
        try {
            if (!loggers.isEmpty()) {
                for (String key : loggers.keySet()) {
                    loggers.get(key).flush();
                }
            }
        } catch (Exception ex) {
            throw new AuditException(ex);
        }
    }

    public AbstractAuditLogger getLogger(String name) {
        if (!Strings.isNullOrEmpty(name) && loggers.containsKey(name)) {
            return loggers.get(name);
        }
        return defaultLogger;
    }

    public AbstractAuditLogger getLogger(Class<? extends IKeyed> type) {
        if (entityIndex.containsKey(type)) {
            return entityIndex.get(type);
        } else if (type.isAnnotationPresent(Audited.class)) {
            Audited a = type.getAnnotation(Audited.class);
            if (loggers.containsKey(a.logger())) {
                return loggers.get(a.logger());
            }
        }
        return defaultLogger;
    }

    public void configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                          @NonNull BaseEnv<?> env) throws ConfigurationException {
        Preconditions.checkArgument(dataStoreManager != null);
        this.env = env;
        try {
            if (!ConfigReader.checkIfNodeExists(xmlConfig, __CONFIG_PATH)) return;

            HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(__CONFIG_PATH);
            String path = ConfigReader.getPathAnnotation(AuditLoggerSettings.class);
            Preconditions.checkState(!Strings.isNullOrEmpty(path));
            List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(path);
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                readLoggerConfig(config);
            }
        } catch (Throwable t) {
            DefaultLogger.error(getClass().getCanonicalName(), t);
            throw new ConfigurationException(t);
        }
    }

    @SuppressWarnings("unchecked")
    private void readLoggerConfig(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            Class<? extends AbstractAuditLogger> cls
                    = (Class<? extends AbstractAuditLogger>) ConfigReader.readAsClass(xmlConfig);

            AbstractAuditLogger<?> logger = ReflectionUtils.createInstance(cls);
            logger.init(xmlConfig, env);
            logger.withDataStoreManager(dataStoreManager);
            loggers.put(logger.name(), logger);
            if (logger.isDefaultLogger()) {
                defaultLogger = logger;
            }
            Set<Class<?>> classes = logger.settings().getClasses();
            if (classes != null && !classes.isEmpty()) {
                for (Class<?> type : classes) {
                    entityIndex.put((Class<? extends IKeyed<?>>) type, logger);
                }
            }
            DefaultLogger.info(String.format("Configured logger. [name=%s][type=%s][default=%s]",
                    logger.name(), logger.getClass().getCanonicalName(), logger.isDefaultLogger()));
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        if (!loggers.isEmpty()) {
            for (String key : loggers.keySet()) {
                loggers.get(key).close();
            }
            loggers.clear();
        }
    }
}
