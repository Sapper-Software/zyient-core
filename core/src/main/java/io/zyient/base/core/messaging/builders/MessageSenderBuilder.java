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

package io.zyient.base.core.messaging.builders;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.messaging.MessageSender;
import io.zyient.base.core.messaging.MessagingError;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class MessageSenderBuilder<I, M> {
    private final Class<? extends MessageSenderSettings> settingsType;
    private BaseEnv<?> env;
    private HierarchicalConfiguration<ImmutableNode> config;

    protected MessageSenderBuilder(@NonNull Class<? extends MessageSenderSettings> settingsType) {
        this.settingsType = settingsType;
    }

    public MessageSenderBuilder<I, M> withEnv(@NonNull BaseEnv<?> env) {
        this.env = env;
        return this;
    }

    public MessageSender<I, M> build(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws MessagingError {
        Preconditions.checkNotNull(env);
        try {
            ConfigReader reader = new ConfigReader(config, settingsType);
            reader.read();
            this.config = reader.config();
            return build((MessageSenderSettings) reader.settings());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new MessagingError(ex);
        }
    }

    public abstract MessageSender<I, M> build(@NonNull MessageSenderSettings settings) throws Exception;
}
