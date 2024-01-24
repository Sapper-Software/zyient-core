/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.core.persistence.errors;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.errors.Error;
import io.zyient.base.core.errors.Errors;
import io.zyient.base.core.errors.ErrorsReader;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.Cursor;
import io.zyient.core.persistence.DataStoreManager;
import io.zyient.core.persistence.env.DataStoreEnv;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StoredErrorsReader implements ErrorsReader {
    private Errors parent;
    private AbstractDataStore<?> dataStore;
    private StoredErrorsReaderSettings settings;

    @Override
    public ErrorsReader withLoader(@NonNull Errors loader) {
        parent = loader;
        return this;
    }

    @Override
    public ErrorsReader configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                  @NonNull BaseEnv<?> env) throws ConfigurationException {
        Preconditions.checkArgument(env instanceof DataStoreEnv<?>);
        try {
            ConfigReader reader = new ConfigReader(xmlConfig, __CONFIG_PATH, StoredErrorsReaderSettings.class);
            reader.read();
            settings = (StoredErrorsReaderSettings) reader.settings();
            DataStoreEnv<?> de = (DataStoreEnv<?>) env;
            DataStoreManager manager = de.dataStoreManager();
            dataStore = manager.getDataStore(settings.getDataStore(), settings.getDataStoreTyp());
            if (dataStore == null) {
                throw new ConfigurationException(String.format("DataStore not found. [name=%s][type=%s]",
                        settings.getDataStore(), settings.getDataStoreTyp().getCanonicalName()));
            }
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public List<Error> read() throws Exception {
        Preconditions.checkNotNull(dataStore);

        String condition = "deleted = :deleted";
        Map<String, Object> params = Map.of("deleted", false);
        AbstractDataStore.Q query = new AbstractDataStore.Q()
                .where(condition)
                .addAll(params);
        Cursor<ErrorKey, ErrorEntity> cursor = dataStore.search(query,
                ErrorKey.class,
                ErrorEntity.class,
                null);
        List<Error> errors = new ArrayList<>();
        while (true) {
            List<ErrorEntity> entities = cursor.nextPage();
            if (entities == null || entities.isEmpty()) break;
            for (ErrorEntity error : entities) {
                errors.add(error.to());
            }
        }
        if (!errors.isEmpty()) {
            return errors;
        }
        return null;
    }
}
