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

package ai.sapper.cdc.core.connections.ws.auth;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.ConnectionError;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Feature;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.JerseyWebTarget;
import org.glassfish.jersey.client.oauth2.OAuth2ClientSupport;

public class WSOAuthHandler extends WebServiceAuthHandler {
    private BaseEnv<?> env;

    @Override
    public WebServiceAuthHandler init(@NonNull WebServiceAuthSettings settings,
                                      @NonNull BaseEnv<?> env) throws ConfigurationException {
        this.settings = settings;
        this.env = env;
        return this;
    }

    @Override
    public WebServiceAuthHandler init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                      @NonNull BaseEnv<?> env) throws ConfigurationException {
        this.env = env;
        ConfigReader reader = new ConfigReader(xmlConfig, WSOAuthSettings.class);
        reader.read();
        this.settings = (WebServiceAuthSettings) reader.settings();
        return this;
    }

    @Override
    public void build(@NonNull JerseyClientBuilder builder) throws Exception {
        Preconditions.checkState(settings instanceof WSOAuthSettings);
        String token = env.keyStore().read(((WSOAuthSettings) settings).getTokenKey());
        if (Strings.isNullOrEmpty(token)) {
            throw new Exception(
                    String.format("Invalid token key: [key=%s]", ((WSOAuthSettings) settings).getTokenKey()));
        }
        Feature feature = OAuth2ClientSupport.feature(token);
        builder.register(feature);
    }

    @Override
    public Invocation.Builder build(@NonNull JerseyWebTarget target,
                                    @NonNull String mediaType) throws ConnectionError {
        return target.request(mediaType);
    }
}
