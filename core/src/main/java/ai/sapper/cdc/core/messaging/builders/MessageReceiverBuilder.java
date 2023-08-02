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

package ai.sapper.cdc.core.messaging.builders;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.messaging.MessageReceiver;
import ai.sapper.cdc.core.messaging.MessagingError;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public abstract class MessageReceiverBuilder<I, M> {
    private final Class<? extends MessageReceiverSettings> settingsType;
    private BaseEnv<?> env;
    protected HierarchicalConfiguration<ImmutableNode> config;

    protected MessageReceiverBuilder(@NonNull Class<? extends MessageReceiverSettings> settingsType) {
        this.settingsType = settingsType;
    }

    public MessageReceiverBuilder<I, M> withEnv(@NonNull BaseEnv<?> env) {
        this.env = env;
        return this;
    }

    public MessageReceiver<I, M> build(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws MessagingError {
        Preconditions.checkNotNull(env);
        try {
            ConfigReader reader = new ConfigReader(config, settingsType);
            reader.read();
            this.config = reader.config();
            return build((MessageReceiverSettings) reader.settings());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new MessagingError(ex);
        }
    }

    public abstract MessageReceiver<I, M> build(@NonNull MessageReceiverSettings settings) throws Exception;
}
