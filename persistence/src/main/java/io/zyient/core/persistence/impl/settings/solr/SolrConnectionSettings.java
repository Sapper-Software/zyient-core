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

package io.zyient.core.persistence.impl.settings.solr;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.lists.StringListParser;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.core.persistence.AbstractConnectionSettings;
import io.zyient.core.persistence.impl.solr.ISolrAuthHandler;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class SolrConnectionSettings extends AbstractConnectionSettings {
    public static final String __CONFIG_PATH = "solr";
    public static class ConstantKeys {
        public static final int DEFAULT_LIVE_CHECK_INTERVAL = 60 * 1000;
        public static final String KEY_LIVE_CHECK_INTERVAL = "LB_LIVE_CHECK";
        public static final int DEFAULT_QUEUE_SIZE = 128;
        public static final String KEY_CONCURRENT_QUEUE_SIZE = "CONCURRENT_QUEUE_SIZE";
        public static final int DEFAULT_THREADS = 4;
        public static final String KEY_CONCURRENT_THREADS = "CONCURRENT_THREADS";
    }

    @Config(name = "client", required = false, type = SolrClientTypes.class)
    private SolrClientTypes clientType = SolrClientTypes.Basic;
    @Config(name = "urls", parser = StringListParser.class)
    private List<String> urls;
    @Config(name = "timeout.connection", required = false, parser = TimeValueParser.class)
    private TimeUnitValue connectionTimeout = new TimeUnitValue(60 * 1000, TimeUnit.MILLISECONDS);
    @Config(name = "timeout.request", required = false, parser = TimeValueParser.class)
    private TimeUnitValue requestTimeout = new TimeUnitValue(10 * 1000, TimeUnit.MILLISECONDS);
    @Config(name = "auth.handler", required = false, type = Class.class)
    private Class<? extends ISolrAuthHandler> authHandler;
}
