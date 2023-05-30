package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.config.ConfigReader;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class MessagingProcessorConfig extends ConfigReader {

    public MessagingProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
        super(config, path, MessagingProcessorSettings.class);
    }

    public MessagingProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
        super(config, MessagingProcessorSettings.class);
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
