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

package io.zyient.base.core.stores.impl.settings.rdbms;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.base.core.stores.AbstractConnectionSettings;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@ConfigPath(path = "hibernate")
public class HibernateConnectionSettings extends AbstractConnectionSettings {
    public static final String CACHE_FACTORY_CLASS = "org.hibernate.cache.ehcache.EhCacheRegionFactory";
    public static final String CACHE_CONFIG_FILE = "net.sf.ehcache.configurationResourceName";
    public static final String CONFIG_HIBERNATE_PATH = "hibernate";
    public static final String CONFIG_C3P0_PATH = "pool";
    public static final String CONFIG_C3P0_PREFIX = "hibernate.c3p0";
    private static final int DEFAULT_POOL_MIN_SIZE = 4;
    private static final int DEFAULT_POOL_MAX_SIZE = 8;
    private static final int DEFAULT_POOL_TIMEOUT = 1800;


    @Config(name = "url")
    private String dbUrl;
    @Config(name = "username")
    private String dbUser;
    @Config(name = "password")
    private String dbPassword;
    @Config(name = "dbname", required = false)
    private String dbName;
    @Config(name = "driver")
    private String driver;
    @Config(name = "dialect")
    private String dialect;
    @Config(name = "enableCaching", required = false, type = Boolean.class)
    private boolean enableCaching = false;
    @Config(name = "enableQueryCaching", required = false, type = Boolean.class)
    private boolean enableQueryCaching = false;
    @Config(name = "cacheConfig", required = false)
    private String cacheConfig;
    @Config(name = "pool.enable", required = false, type = Boolean.class)
    private boolean enableConnectionPool = false;
    @Config(name = "pool.minSize", required = false, type = Integer.class)
    private int poolMinSize = DEFAULT_POOL_MIN_SIZE;
    @Config(name = "pool.maxSize", required = false, type = Integer.class)
    private int poolMaxSize = DEFAULT_POOL_MAX_SIZE;
    @Config(name = "pool.timeout", required = false, parser = TimeValueParser.class)
    private TimeUnitValue poolTimeout = new TimeUnitValue(DEFAULT_POOL_TIMEOUT, TimeUnit.MILLISECONDS);
    @Config(name = "pool.check", required = false, type = Boolean.class)
    private boolean poolConnectionCheck = true;
    @Config(name = "config", required = false)
    private String hibernateConfigSource;
}
