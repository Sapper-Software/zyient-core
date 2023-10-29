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

package io.zyient.base.core.stores.impl.rdbms;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.cache.ThreadCache;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.stores.AbstractConnection;
import io.zyient.base.core.stores.impl.settings.rdbms.HibernateConnectionSettings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.service.ServiceRegistry;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

@Getter
@Accessors(fluent = true)
public class HibernateConnection extends AbstractConnection<Session> {
    @Setter(AccessLevel.NONE)
    private SessionFactory sessionFactory = null;
    @Getter(AccessLevel.NONE)
    private final ThreadCache<Session> threadCache = new ThreadCache<>();
    private BaseEnv<?> env;

    public HibernateConnection() {
        super(EConnectionType.db, HibernateConnectionSettings.class);
    }

    public HibernateConnection withSessionFactory(@NonNull SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
        return this;
    }

    @Override
    public boolean hasTransactionSupport() {
        return true;
    }

    @Override
    public void close(@NonNull Session connection) throws ConnectionError {
        try {
            if (connection.isOpen()) {
                connection.close();
            } else {
                DefaultLogger.warn("Connection already closed...");
            }

            Session cs = threadCache.remove();
            if (cs == null) {
                throw new ConnectionError("Connection not created via connection manager...");
            }
            if (!cs.equals(connection)) {
                throw new ConnectionError("Connection handle passed doesn't match cached connection.");
            }
        } catch (Exception ex) {
            DefaultLogger.error(ex.getLocalizedMessage());
            throw new ConnectionError(ex);
        }
    }

    public Session getConnection() throws ConnectionError {
        state().check(EConnectionState.Connected);
        try {
            if (threadCache.contains()) {
                Session session = threadCache.get();
                if (session.isOpen()) return session;
            }
            synchronized (threadCache) {
                Session session = sessionFactory.openSession();
                return threadCache.put(session);
            }
        } catch (Throwable t) {
            throw new ConnectionError(t);
        }
    }

    /**
     * Configure this type instance.
     *
     * @param settings - Handle to the configuration node.
     * @throws ConfigurationException
     */
    private void configure(@NonNull HibernateConnectionSettings settings) throws ConfigurationException {
        try {
            String passwd = env.keyStore().read(settings.getDbPassword());
            if (Strings.isNullOrEmpty(passwd)) {
                throw new ConfigurationException(
                        String.format("DataStore password not found. [key=%s]", settings.getDbPassword()));
            }
            if (!Strings.isNullOrEmpty(settings.getHibernateConfigSource())) {
                File cfg = new File(settings.getHibernateConfigSource());
                if (!cfg.exists()) {
                    throw new ConfigurationException(String.format("Hibernate configuration not found. [path=%s]", cfg.getAbsolutePath()));
                }
                Properties properties = new Properties();
                properties.setProperty(Environment.PASS, passwd);
                sessionFactory = new Configuration().configure(cfg).addProperties(properties).buildSessionFactory();
            } else {
                Configuration configuration = new Configuration();

                Properties properties = new Properties();
                properties.setProperty(Environment.DRIVER, settings.getDriver());
                properties.setProperty(Environment.URL, settings.getDbUrl());
                properties.setProperty(Environment.USER, settings.getDbUser());
                properties.setProperty(Environment.PASS, passwd);
                properties.setProperty(Environment.DIALECT, settings.getDialect());

                if (settings.isEnableConnectionPool()) {
                    if (settings.getPoolMinSize() < 0
                            || settings.getPoolMaxSize() <= 0
                            || settings.getPoolMaxSize() < settings.getPoolMinSize()) {
                        throw new ConfigurationException(
                                String.format("Invalid Pool Configuration : [min size=%d][max size=%d",
                                        settings.getPoolMinSize(), settings.getPoolMaxSize()));
                    }
                    properties.setProperty(
                            HibernateConnectionSettings.CONFIG_C3P0_PREFIX + ".min_size",
                            String.valueOf(settings.getPoolMinSize()));
                    properties.setProperty(
                            HibernateConnectionSettings.CONFIG_C3P0_PREFIX + ".max_size",
                            String.valueOf(settings.getPoolMaxSize()));
                    if (settings.getPoolTimeout().normalized() > 0)
                        properties.setProperty(
                                HibernateConnectionSettings.CONFIG_C3P0_PREFIX + ".timeout",
                                String.valueOf(settings.getPoolTimeout().normalized()));
                    if (settings.isPoolConnectionCheck()) {
                        properties.setProperty(
                                HibernateConnectionSettings.CONFIG_C3P0_PREFIX + ".testConnectionOnCheckout", "true");
                    }
                }
                if (settings.isEnableCaching()) {
                    if (Strings.isNullOrEmpty(settings.getCacheConfig())) {
                        throw new ConfigurationException("Missing cache configuration file. ");
                    }
                    properties.setProperty(Environment.USE_SECOND_LEVEL_CACHE, "true");
                    properties.setProperty(Environment.CACHE_REGION_FACTORY,
                            HibernateConnectionSettings.CACHE_FACTORY_CLASS);
                    if (settings.isEnableQueryCaching())
                        properties.setProperty(Environment.USE_QUERY_CACHE, "true");
                    properties.setProperty(HibernateConnectionSettings.CACHE_CONFIG_FILE, settings.getCacheConfig());
                }
                if (settings.getParameters() != null && !settings.getParameters().isEmpty()) {
                    for (String key : settings.getParameters().keySet()) {
                        properties.setProperty(key, settings.getParameters().get(key));
                    }
                }
                configuration.setProperties(properties);

                if (settings.getSupportedTypes() != null && !settings.getSupportedTypes().isEmpty()) {
                    for (Class<?> cls : settings.getSupportedTypes()) {
                        configuration.addAnnotatedClass(cls);
                    }
                }

                ServiceRegistry registry = new StandardServiceRegistryBuilder()
                        .applySettings(configuration.getProperties()).build();
                sessionFactory = configuration.buildSessionFactory(registry);

                state().setState(EConnectionState.Initialized);
            }
        } catch (Exception ex) {
            state().error(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        if (state().isConnected())
            state().setState(EConnectionState.Closed);
        threadCache.close();
        if (sessionFactory != null) {
            sessionFactory.close();
            sessionFactory = null;
        }
    }

    public Transaction startTransaction() throws ConnectionError {
        Session session = getConnection();
        Transaction tx = null;
        if (session.isJoinedToTransaction()) {
            tx = session.getTransaction();
        } else {
            tx = session.beginTransaction();
        }
        return tx;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkState(settings instanceof HibernateConnectionSettings);
        try {
            this.env = env;
            configure((HibernateConnectionSettings) settings);
            return this;
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection connect() throws ConnectionError {
        if (!state().isConnected()) {
            state().check(EConnectionState.Initialized);
            state().setState(EConnectionState.Connected);
        }
        return this;
    }
}
