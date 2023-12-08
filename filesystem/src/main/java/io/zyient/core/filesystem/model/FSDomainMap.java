/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.core.filesystem.model;

import lombok.NonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
