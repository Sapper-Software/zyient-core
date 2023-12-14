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

package io.zyient.core.mapping.rules.drools;

import com.google.common.base.Preconditions;
import io.zyient.core.mapping.model.MappedResponse;
import io.zyient.core.mapping.rules.Rule;
import io.zyient.core.mapping.rules.RuleConfig;
import io.zyient.core.mapping.rules.RuleType;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.ReleaseId;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;

import java.io.File;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class DroolsRule<T> implements Rule<T> {
    private File contentDir;
    private DroolsConfig config;
    private boolean ignoreRecordOnCondition = false;
    private Class<? extends T> entityType;
    private final KieServices services = KieServices.Factory.get();
    private KieContainer container;

    @Override
    public String name() {
        Preconditions.checkNotNull(config);
        return config.getName();
    }

    @Override
    public Rule<T> withContentDir(@NonNull File contentDir) {
        this.contentDir = contentDir;
        return this;
    }

    @Override
    public Rule<T> withEntityType(@NonNull Class<? extends T> type) {
        entityType = type;
        return this;
    }

    @Override
    public Rule<T> configure(@NonNull RuleConfig config) throws ConfigurationException {
        Preconditions.checkArgument(config instanceof DroolsConfig);
        ignoreRecordOnCondition = config.isIgnoreRecordOnCondition();
        this.config = (DroolsConfig) config;
        createContainer();
        return this;
    }

    private KieFileSystem getKieFileSystem() {
        KieFileSystem fileSystem = services.newKieFileSystem();
        List<String> rules = config.getDrls();
        for (String rule : rules) {
            fileSystem.write(ResourceFactory.newClassPathResource(rule));
        }
        return fileSystem;
    }

    private void createContainer() {
        KieBuilder kb = services.newKieBuilder(getKieFileSystem());
        kb.buildAll();

        KieRepository kieRepository = services.getRepository();
        ReleaseId krDefaultReleaseId = kieRepository.getDefaultReleaseId();
        container = services.newKieContainer(krDefaultReleaseId);
    }

    @Override
    public Object evaluate(@NonNull MappedResponse<T> data) throws Exception {
        KieSession session = container.newKieSession();
        try {
            session.insert(data);
            return session.fireAllRules();
        } finally {
            session.destroy();
        }
    }

    @Override
    public RuleType getRuleType() {
        return RuleType.Transformation;
    }

    @Override
    public void addSubRules(@NonNull List<Rule<T>> rules) throws Exception {
        throw new Exception("Not supported...");
    }
}
