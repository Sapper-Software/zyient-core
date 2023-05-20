package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.config.ConfigValueParser;
import ai.sapper.cdc.common.utils.DefaultLogger;
import com.google.common.base.Strings;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

public class KafkaPartitionsParser implements ConfigValueParser<List<Integer>> {
    public List<Integer> parse(@NonNull String value) throws Exception {
        List<Integer> partitions = new ArrayList<>();
        if (!Strings.isNullOrEmpty(value)) {
            if (value.indexOf(';') >= 0) {
                String[] parts = value.split(";");
                for (String part : parts) {
                    if (Strings.isNullOrEmpty(part)) continue;
                    Integer p = Integer.parseInt(part.trim());
                    partitions.add(p);
                    DefaultLogger.debug(String.format("Added partition; [%d]", p));
                }
            } else {
                Integer p = Integer.parseInt(value.trim());
                partitions.add(p);
                DefaultLogger.debug(String.format("Added partition; [%d]", p));
            }
        }
        if (partitions.isEmpty()) partitions.add(0);
        return partitions;
    }

    public String serialize(@NonNull List<Integer> source) throws Exception {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (int v : source) {
            if (first) first = false;
            else {
                builder.append(";");
            }
            builder.append(v);
        }
        return builder.toString();
    }
}
