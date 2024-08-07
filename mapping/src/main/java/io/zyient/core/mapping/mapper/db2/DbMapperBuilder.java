package io.zyient.core.mapping.mapper.db2;

import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.model.StringKey;
import io.zyient.core.mapping.readers.impl.db.QueryBuilder;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.Cursor;
import io.zyient.core.persistence.DataStoreException;
import io.zyient.core.persistence.env.DataStoreEnv;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.springframework.util.StringUtils;

import java.util.*;

public class DbMapperBuilder implements IDbMapperBuilder {

    private AbstractDataStore<?> dataStore;
    private QueryBuilder builder;
    private DbMapperSettings settings;

    @Override
    public IDbMapperBuilder configure(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                      @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(config, DbMapperSettings.class);
            reader.read();
            settings = (DbMapperSettings) reader.settings();
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
    public DBMapper build(Context context) throws Exception {
        List<DBMappingConf> globalConditions = getGlobalConfigs(context);
        List<DBMappingConf> filteredConditions = getFilterConfigs(context);
        List<DBMappingConf> finalConditions = new ArrayList<>();
        for (DBMappingConf condition : globalConditions) {
            DBMappingConf actualConf = condition;
            for (DBMappingConf filter : filteredConditions) {
                if (isConfEquals(condition, filter)) {
                    actualConf = filter;
                }
            }
            String source = settings.getSourceAsSource() ? actualConf.getSourcePath() : actualConf.getTargetPath();
            String target = settings.getSourceAsSource() ? actualConf.getTargetPath() : actualConf.getSourcePath();
            actualConf.setSourcePath(source);
            actualConf.setTargetPath(target);
            finalConditions.add(actualConf);
        }
        return new DBMapper(finalConditions);
    }

    private boolean isConfEquals(DBMappingConf conf, DBMappingConf conf2) {
        return settings.getSourceAsSource() ? conf.getSourcePath().equals(conf2.getSourcePath()) :
                conf.getTargetPath().equals(conf2.getTargetPath());
    }

    private List<DBMappingConf> getGlobalConfigs(Context context) throws Exception {
        AbstractDataStore.Q query = buildQuery(context);
        return getConfigs(query);
    }

    private List<DBMappingConf> getFilterConfigs(Context context) throws Exception {
        AbstractDataStore.Q query = buildFilterQuery(context);
        return getConfigs(query);
    }

    private List<DBMappingConf> getConfigs(AbstractDataStore.Q query) throws DataStoreException {
        Cursor<StringKey, DBMappingConf> cursor = dataStore.search(query,
                StringKey.class,
                settings.getConditionType(),
                null);
        List<DBMappingConf> records = new ArrayList<>();
        while (true) {
            List<DBMappingConf> conditions = cursor.nextPage();
            if (conditions == null || conditions.isEmpty()) break;
            records.addAll(conditions);
        }
        return records;
    }

    private AbstractDataStore.Q buildQuery(Context context) throws Exception {
        return buildCoreQuery(settings.getMappingQuery(), context);

    }

    private AbstractDataStore.Q buildCoreQuery(String query, Context context) throws Exception {
        Map<String, Object> conditions = null;
        if (!Strings.isNullOrEmpty(settings.getCondition())) {
            conditions = JSONUtils.read(settings.getCondition(), Map.class);
            conditions = buildConditions(query, conditions, context);
        }
        return builder.build(query, conditions);
    }

    private AbstractDataStore.Q buildFilterQuery(Context context) throws Exception {
        return buildCoreQuery(settings.getFilterQuery(), context);
    }

    private List<String> getRequiredParams(String query) {
        List<String> result = new ArrayList<>();
        String[] tokens = query.split(" ");
        for (String token : tokens) {
            if (token.trim().startsWith(":")) {
                result.add(token.trim());
            }
        }
        return result;
    }

    private Map<String, Object> buildConditions(String query, Map<String, Object> conditions, Context context) throws Exception {
        List<String> requiredParams = getRequiredParams(query);
        Map<String, Object> updated = new HashMap<>();
        for (String key : conditions.keySet()) {
            Object value = conditions.get(key);
            if (value instanceof String str) {
                if (str.startsWith(":") && requiredParams.contains(str)) {
                    String name = str.substring(1);
                    if (context == null) {
                        throw new Exception(String.format("context values expected for condition. [condition=%s]",
                                settings.getCondition()));
                    }
                    value = context.get(key);
                    if (value == null) {
                        throw new Exception(String.format("Missing context value. [context=%s]", name));
                    }
                    updated.put(key, value);
                }
            }
        }
        return updated;
    }
}
