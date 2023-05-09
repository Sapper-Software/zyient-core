package ai.sapper.cdc.core.io.model;

import lombok.NonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FSDomainMap {
    public static final String __DEFAULT_DOMAIN = "default";
    private final Map<String, Container> domains = new HashMap<>();

    public FSDomainMap(Container defaultContainer, Map<String, Container> domains) {
        if (domains != null && !domains.isEmpty()) {
            this.domains.putAll(domains);
        }
        this.domains.put(__DEFAULT_DOMAIN, defaultContainer);
    }

    public Container get(@NonNull String domain) {
        if (domains.containsKey(domain)) {
            return domains.get(domain);
        }
        return domains.get(__DEFAULT_DOMAIN);
    }

    public Collection<Container> getDomains() {
        return this.domains.values();
    }
}
