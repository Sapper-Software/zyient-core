package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverBuilder;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverSettings;
import ai.sapper.cdc.core.messaging.builders.MessageSenderBuilder;
import ai.sapper.cdc.core.messaging.builders.MessageSenderSettings;
import ai.sapper.cdc.core.processing.ProcessorSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class MessagingConfigSettings extends ProcessorSettings {
    public static class Constants {
        public static final String __CONFIG_PATH_RECEIVER = "queue";
        public static final String __CONFIG_PATH_ERRORS = "errors";

        public static final String CONFIG_BUILDER_TYPE = "builder.type";
        public static final String CONFIG_MESSAGING_SETTINGS_TYPE = "builder.settingsType";
        public static final String CONFIG_ERRORS_BUILDER_TYPE = "errors.builder.type";
        public static final String CONFIG_ERRORS_MESSAGING_SETTINGS_TYPE = "errors.builder.settingsType";
        public static final String CONFIG_BATCH_RECEIVE_TIMEOUT = "readBatchTimeout";
    }

    @Config(name = Constants.CONFIG_BUILDER_TYPE, type = Class.class)
    private Class<? extends MessageReceiverBuilder<?, ?>> builderType;
    @Config(name = Constants.CONFIG_MESSAGING_SETTINGS_TYPE, type = Class.class)
    private Class<? extends MessageReceiverSettings> builderSettingsType;
    @Config(name = Constants.CONFIG_BATCH_RECEIVE_TIMEOUT, required = false, type = Long.class)
    private long receiveBatchTimeout = 1000;
    @Config(name = Constants.CONFIG_ERRORS_BUILDER_TYPE, required = false, type = Class.class)
    private Class<? extends MessageSenderBuilder<?, ?>> errorsBuilderType;
    @Config(name = Constants.CONFIG_ERRORS_MESSAGING_SETTINGS_TYPE, required = false, type = Class.class)
    private Class<? extends MessageSenderSettings> errorsBuilderSettingsType;
}
