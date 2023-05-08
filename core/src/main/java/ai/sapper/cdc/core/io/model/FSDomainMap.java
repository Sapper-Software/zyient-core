package ai.sapper.cdc.core.io.model;

import lombok.NonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FSDomainMap {
    public static final String __DEFAULT_DOMAIN = "default";
    private final Map<String, String> domains = new HashMap<>();

    public FSDomainMap(String defaultDomain, Map<String, String> domains) {
        if (domains != null && !domains.isEmpty()) {
            this.domains.putAll(domains);
        }
        this.domains.put(__DEFAULT_DOMAIN, defaultDomain);
    }

    public String get(@NonNull String domain) {
        if (domains.containsKey(domain)) {
            return domains.get(domain);
        }
        return domains.get(__DEFAULT_DOMAIN);
    }

    public Collection<String> getDomains() {
        return this.domains.values();
    }
}
