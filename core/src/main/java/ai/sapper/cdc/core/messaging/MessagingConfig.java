package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.config.ConfigReader;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class MessagingConfig extends ConfigReader {

    public MessagingConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
        super(config, path, MessagingConfigSettings.class);
    }

    public MessagingConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
        super(config, MessagingConfigSettings.class);
    }

    @Override
    public void read() throws ConfigurationException {
        super.read();
        MessagingConfigSettings settings = (MessagingConfigSettings) settings();
        if (settings.getErrorsBuilderType() != null) {
            if (settings.getErrorsBuilderSettingsType() == null) {
                throw new ConfigurationException("Error Queue builder specified, but builder settings not specified.");
            }
        }
    }
}
