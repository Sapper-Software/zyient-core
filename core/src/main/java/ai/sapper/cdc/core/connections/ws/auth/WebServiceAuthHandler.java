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

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.ConnectionError;
import jakarta.ws.rs.client.Invocation;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.JerseyWebTarget;

@Getter
@Accessors(fluent = true)
public abstract class WebServiceAuthHandler {
    protected WebServiceAuthSettings settings;

    public abstract WebServiceAuthHandler init(@NonNull WebServiceAuthSettings settings,
                                               @NonNull BaseEnv<?> env) throws ConfigurationException;

    public abstract WebServiceAuthHandler init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                               @NonNull BaseEnv<?> env) throws ConfigurationException;

    public abstract void build(@NonNull JerseyClientBuilder builder) throws Exception;

    public abstract Invocation.Builder build(@NonNull JerseyWebTarget target,
                                             @NonNull String mediaType) throws ConnectionError;
}
