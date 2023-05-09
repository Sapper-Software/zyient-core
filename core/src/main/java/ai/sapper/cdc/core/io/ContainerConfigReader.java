package ai.sapper.cdc.core.io;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.io.model.Container;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class ContainerConfigReader extends ConfigReader {
    public static final String __CONFIG_PATH = "containers";
    public static final String CONFIG_CONTAINER = "container";
    public static final String CONFIG_DEFAULT_CONTAINER = "default";

    private Map<String, Container> containers;
    private final ConfigReader reader;
    private Container defaultContainer;

    public ContainerConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                 @NonNull Class<? extends Container> containerType) {
        super(config, __CONFIG_PATH, containerType);
        reader = new ConfigReader(config, __CONFIG_PATH, containerType);
    }

    @Override
    public void read() throws ConfigurationException {
        try {
            List<HierarchicalConfiguration<ImmutableNode>> nodes = config().configurationsAt(CONFIG_CONTAINER);
            if (nodes != null && !nodes.isEmpty()) {
                containers = new HashMap<>();
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    Settings settings = type().getDeclaredConstructor().newInstance();
                    Preconditions.checkState(settings instanceof Container);
                    settings = reader.read(settings, node);
                    Container container = (Container) settings;
                    containers.put(container.getDomain(), container);
                }
            } else {
                throw new ConfigurationException("No containers defined...");
            }
            String key = get().getString(CONFIG_DEFAULT_CONTAINER);
            checkStringValue(key, getClass(), CONFIG_DEFAULT_CONTAINER);
            defaultContainer = containers.get(key);
            if (defaultContainer == null) {
                throw new ConfigurationException(String.format("Invalid Default Container: [name=%s]", key));
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
