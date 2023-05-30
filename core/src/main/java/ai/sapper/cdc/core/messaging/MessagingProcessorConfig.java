package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.config.ConfigReader;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class MessagingProcessorConfig extends ConfigReader {
    public MessagingProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                    @NonNull String path,
                                    @NonNull Class<? extends MessagingProcessorSettings> settingsType) {
        super(config, path, settingsType);
    }

    public MessagingProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                    @NonNull Class<? extends MessagingProcessorSettings> settingsType) {
        super(config, settingsType);
    }

    @Override
    public void read() throws ConfigurationException {
        super.read();
        MessagingProcessorSettings settings = (MessagingProcessorSettings) settings();
        if (settings.getErrorsBuilderType() != null) {
            if (settings.getErrorsBuilderSettingsType() == null) {
                throw new ConfigurationException("Error Queue builder specified, but builder settings not specified.");
            }
        }
    }
}
