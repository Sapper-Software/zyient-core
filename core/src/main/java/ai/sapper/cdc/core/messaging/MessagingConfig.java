package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.Settings;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class MessagingConfig extends ConfigReader {

    public MessagingConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
        super(config, path, MessagingConfigSettings.class);
    }
}
