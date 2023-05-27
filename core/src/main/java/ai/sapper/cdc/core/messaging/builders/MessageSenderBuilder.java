package ai.sapper.cdc.core.messaging.builders;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.cdc.core.messaging.MessagingError;
import com.google.common.base.Preconditions;
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
    private final BaseEnv<?> env;
    private HierarchicalConfiguration<ImmutableNode> config;

    protected MessageSenderBuilder(@NonNull BaseEnv<?> env,
                                   @NonNull Class<? extends MessageSenderSettings> settingsType) {
        this.env = env;
        this.settingsType = settingsType;
    }

    public MessageSender<I, M> build(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws MessagingError {
        try {
            ConfigReader reader = new ConfigReader(config, settingsType);
            reader.read();
            this.config = reader.config();
            return build((MessageSenderSettings) reader.settings());
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    public abstract MessageSender<I, M> build(@NonNull MessageSenderSettings settings) throws Exception;
}
