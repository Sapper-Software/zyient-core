package io.zyient.core.mapping.rules.db;

import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.FieldValueParser;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldMappingReader implements FieldValueParser<Map<String, String>> {
    public static final String __CONFIG_PATH = "fieldMappings";
    public static final String CONFIG_NODE = "mapping";
    public static final String CONFIG_SOURCE = "source";
    public static final String CONFIG_TARGET = "target";

    @Override
    public Map<String, String> parse(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        if (ConfigReader.checkIfNodeExists(config, __CONFIG_PATH)) {
            Map<String, String> mappings = new HashMap<>();
            HierarchicalConfiguration<ImmutableNode> node = config.configurationAt(__CONFIG_PATH);
            List<HierarchicalConfiguration<ImmutableNode>> nodes = node.configurationsAt(CONFIG_NODE);
            for (HierarchicalConfiguration<ImmutableNode> n : nodes) {
                String key = n.getString(CONFIG_SOURCE);
                String value = n.getString(CONFIG_TARGET);
                if (Strings.isNullOrEmpty(key) || Strings.isNullOrEmpty(value)) {
                    throw new ConfigurationException("Invalid configuration: Missing Key and/or Value.");
                }
                mappings.put(key, value);
            }
            if (!mappings.isEmpty()) {
                return mappings;
            }
        }
        return null;
    }
}
