package io.zyient.core.mapping.config;

import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigValueParser;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RegexGroupParser implements ConfigValueParser<Map<Integer, List<Integer>>> {
    @Override
    public Map<Integer, List<Integer>> parse(@NonNull String value) throws Exception {
        if (!Strings.isNullOrEmpty(value)) {
            String[] parts = value.split(",");
            if (parts.length > 0) {
                Map<Integer, List<Integer>> values = new HashMap<>();
                for (String part : parts) {
                    String[] vs = part.split(":");
                    if (vs.length != 2) {
                        throw new Exception(String.format("Invalid entry: %s", part));
                    }
                    int m = Integer.parseInt(vs[0]);
                    int g = Integer.parseInt(vs[1]);
                    List<Integer> list = null;
                    if (values.containsKey(m)) {
                        list = values.get(m);
                    } else {
                        list = new ArrayList<>();
                        values.put(m, list);
                    }
                    list.add(g);
                }
                return values;
            }
        }
        return null;
    }

    @Override
    public String serialize(@NonNull Map<Integer, List<Integer>> value) throws Exception {
        return null;
    }
}
