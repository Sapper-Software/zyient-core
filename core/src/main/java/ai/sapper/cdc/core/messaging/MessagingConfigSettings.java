package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverBuilder;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class MessagingConfigSettings extends Settings {
    public static class Constants {
        public static final String CONFIG_BUILDER_TYPE = "builder.type";
        public static final String CONFIG_MESSAGING_SETTINGS_TYPE = "builder.settingsType";
    }

    @Config(name = Constants.CONFIG_BUILDER_TYPE, type = Class.class)
    private Class<? extends MessageReceiverBuilder<?, ?>> builderType;
    @Config(name = Constants.CONFIG_MESSAGING_SETTINGS_TYPE, type = Class.class)
    private Class<? extends MessageReceiverSettings> settingsType;
}
