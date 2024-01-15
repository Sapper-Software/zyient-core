/*
 * Copyright(C) (2024) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.core.mapping.mapper.db;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.entity.EDataTypes;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.decisions.Condition;
import io.zyient.base.core.decisions.ConditionParser;
import io.zyient.base.core.decisions.EvaluationTree;
import io.zyient.base.core.decisions.builder.EvaluationTreeBuilder;
import io.zyient.base.core.model.StringKey;
import io.zyient.core.mapping.model.mapping.ConditionalMappedElement;
import io.zyient.core.mapping.readers.impl.db.QueryBuilder;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.Cursor;
import io.zyient.core.persistence.env.DataStoreEnv;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DbEvaluationTreeBuilder implements EvaluationTreeBuilder<Map<String, Object>, ConditionalMappedElement> {
    private AbstractDataStore<?> dataStore;
    private DbEvaluationTreeBuilderSettings settings;
    private QueryBuilder builder;

    @Override
    public DbEvaluationTreeBuilder configure(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                             @NonNull BaseEnv<?> env) throws ConfigurationException {
        Preconditions.checkArgument(env instanceof DataStoreEnv<?>);
        try {
            ConfigReader reader = new ConfigReader(config, DbEvaluationTreeBuilderSettings.class);
            reader.read();
            settings = (DbEvaluationTreeBuilderSettings) reader.settings();
            dataStore = ((DataStoreEnv<?>) env).getDataStoreManager()
                    .getDataStore(settings.getDataStore(), settings.getDataStoreType());
            if (dataStore == null) {
                throw new Exception(String.format("DataStore not found. [name=%s][type=%s]",
                        settings.getDataStore(), settings.getDataStoreType().getCanonicalName()));
            }
            builder = settings.getBuilder()
                    .getDeclaredConstructor()
                    .newInstance();
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public EvaluationTree<Map<String, Object>, ConditionalMappedElement> build() throws Exception {
        AbstractDataStore.Q query = buildQuery();
        Cursor<StringKey, DBConditionDef> cursor = dataStore.search(query,
                StringKey.class,
                settings.getConditionType(),
                null);
        Map<String, DBConditionDef> records = new HashMap<>();
        while (true) {
            List<DBConditionDef> conditions = cursor.nextPage();
            if (conditions == null || conditions.isEmpty()) break;
            for (DBConditionDef condition : conditions) {
                records.put(condition.getId().stringKey(), condition);
            }
        }
        if (!records.isEmpty()) {
            EvaluationTree<Map<String, Object>, ConditionalMappedElement> tree = new EvaluationTree<>();
            Map<String, EvaluationTree.Node<Map<String, Object>, ConditionalMappedElement>> nodes = new HashMap<>();
            ConditionParser<Map<String, Object>> parser = (ConditionParser<Map<String, Object>>) settings.getParser()
                    .getDeclaredConstructor()
                    .newInstance();
            while (!records.isEmpty()) {
                List<DBConditionDef> delete = new ArrayList<>();
                for (String key : records.keySet()) {
                    DBConditionDef def = records.get(key);
                    Condition<Map<String, Object>> condition = parser.parse(def.getCondition(),
                            def.getField(),
                            EDataTypes.asJavaType(def.getType()));
                    ConditionalMappedElement me = null;
                    if (def.getMappings() != null && !def.getMappings().isEmpty()) {
                        me = new ConditionalMappedElement();
                        for (DBMappingDef md : def.getMappings()) {
                            me.add(md.as());
                        }
                    }
                    if (def.getParentId() == null) {
                        EvaluationTree.Node<Map<String, Object>, ConditionalMappedElement> node
                                = tree.add(condition, me);
                        nodes.put(def.getId().stringKey(), node);
                        delete.add(def);
                    } else {
                        if (nodes.containsKey(def.getId().stringKey())) {
                            EvaluationTree.Node<Map<String, Object>, ConditionalMappedElement> node
                                    = nodes.get(def.getId().stringKey());
                            EvaluationTree.Node<Map<String, Object>, ConditionalMappedElement> cnode
                                    = node.add(condition, me);
                            nodes.put(def.getId().stringKey(), cnode);
                            delete.add(def);
                        }
                    }
                }
                if (!delete.isEmpty()) {
                    for (DBConditionDef def : delete) {
                        records.remove(def.getId().stringKey());
                    }
                }
            }
            return tree;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private AbstractDataStore.Q buildQuery() throws Exception {
        Map<String, Object> conditions = null;
        if (!Strings.isNullOrEmpty(settings.getCondition())) {
            conditions = JSONUtils.read(settings.getCondition(), Map.class);
        }
        return builder.build(settings.getQuery(), conditions);
    }
}
