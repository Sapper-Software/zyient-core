package io.zyient.core.mapping.rules.collection;

import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.FieldValueParser;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.List;

public class ListRuleReader implements FieldValueParser<List<String>> {
    public static final String __CONFIG_PATH = "items";
    public static final String CONFIG_NODE = "item";

    @Override
    public List<String> parse(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        if (ConfigReader.checkIfNodeExists(config, __CONFIG_PATH)) {

            HierarchicalConfiguration<ImmutableNode> node = config.configurationAt(__CONFIG_PATH);
            List<String> items = node.getList(String.class, CONFIG_NODE);
            if (!items.isEmpty()) {
                return items;
            }
        }
        return null;
    }
}
