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

package io.zyient.base.core.connections.ws.auth;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.ConnectionError;
import jakarta.ws.rs.client.Invocation;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.JerseyWebTarget;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

public class WSBasicAuthHandler extends WebServiceAuthHandler {
    protected BaseEnv<?> env;

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
        ConfigReader reader = new ConfigReader(xmlConfig, WSBasicAuthSettings.class);
        reader.read();
        this.settings = (WebServiceAuthSettings) reader.settings();
        return this;
    }

    @Override
    public void build(@NonNull JerseyClientBuilder builder) throws Exception {
        HttpAuthenticationFeature feature = HttpAuthenticationFeature.basicBuilder().build();
        builder.register(feature);
    }

    @Override
    public Invocation.Builder build(@NonNull JerseyWebTarget target,
                                    @NonNull String mediaType) throws ConnectionError {
        Preconditions.checkState(settings instanceof WSBasicAuthSettings);
        try {
            String passwd = env.keyStore().read(((WSBasicAuthSettings) settings).getPassKey());
            if (Strings.isNullOrEmpty(passwd)) {
                throw new ConnectionError(String.format("Password not found. [key=%s]",
                        ((WSBasicAuthSettings) settings).getPassKey()));
            }
            return target.request(mediaType)
                    .property(HttpAuthenticationFeature.HTTP_AUTHENTICATION_BASIC_USERNAME,
                            ((WSBasicAuthSettings) settings).getUsername())
                    .property(HttpAuthenticationFeature.HTTP_AUTHENTICATION_BASIC_PASSWORD, passwd);
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
    }
}
