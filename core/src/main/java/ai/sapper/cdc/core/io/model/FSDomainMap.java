package ai.sapper.cdc.core.io.model;

import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;

public class FSDomainMap {
    public static final String __DEFAULT_DOMAIN = "default";
    private final Map<String, String> domains = new HashMap<>();

    public FSDomainMap(Map<String, String> domains) {
        if (domains != null && !domains.isEmpty()) {
            this.domains.putAll(domains);
        }
    }

    public String get(@NonNull String domain) {
        if (domains.containsKey(domain)) {
            return domains.get(domain);
        }
        return __DEFAULT_DOMAIN;
    }
}
