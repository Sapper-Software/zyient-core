package io.zyient.base.common.config.maps;

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


public abstract class MapFieldValueParser<V> implements FieldValueParser<Map<String, V>> {
    public static final String __CONFIG_PATH = "map";
    public static final String __CONFIG_PATH_VALUES = "values";
    public static final String KEY_NAME = "name";
    public static final String KEY_VALUE = "value";

    private final String path;
    private final String valuesPath;
    private final String keyName;
    private final String valueName;

    public MapFieldValueParser() {
        path = __CONFIG_PATH;
        valuesPath = __CONFIG_PATH_VALUES;
        keyName = KEY_NAME;
        valueName = KEY_VALUE;
    }

    public MapFieldValueParser(String path,
                               @NonNull String valuesPath,
                               @NonNull String keyName,
                               @NonNull String valueName) {
        this.path = path;
        this.valuesPath = valuesPath;
        this.keyName = keyName;
        this.valueName = valueName;
    }


    @Override
    public Map<String, V> parse(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        try {
            if (Strings.isNullOrEmpty(path) || ConfigReader.checkIfNodeExists(config, path)) {
                Map<String, V> map = new HashMap<>();
                HierarchicalConfiguration<ImmutableNode> node = config.configurationAt(path);
                List<HierarchicalConfiguration<ImmutableNode>> nodes = node.configurationsAt(valuesPath);
                for (HierarchicalConfiguration<ImmutableNode> n : nodes) {
                    String key = n.getString(keyName).trim();
                    String value = n.getString(valueName).trim();
                    if (Strings.isNullOrEmpty(key) || Strings.isNullOrEmpty(value)) {
                        throw new Exception("Invalid Map configuration: Key and/or Value missing...");
                    }
                    map.put(key, fromString(value));
                }
                if (!map.isEmpty()) {
                    return map;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    protected abstract V fromString(@NonNull String value) throws Exception;
}
