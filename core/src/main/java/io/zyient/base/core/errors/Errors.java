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

package io.zyient.base.core.errors;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class Errors {
    public static final String __CONFIG_PATH = "errors";
    public static final String __CONFIG_PATH_LOADER = "loader";

    private final Map<String, Map<Integer, Error>> errors = new HashMap<>();

    @SuppressWarnings("unchecked")
    public void configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                          @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(__CONFIG_PATH_LOADER);
            Class<? extends ErrorsReader> type = (Class<? extends ErrorsReader>) ConfigReader.readType(config);
            if (type == null) {
                throw new ConfigurationException("Errors Reader not specified...");
            }
            ErrorsReader r = type.getDeclaredConstructor()
                    .newInstance()
                    .withLoader(this)
                    .configure(config, env);
            List<Error> result = r.read();
            if (result != null && !result.isEmpty()) {
                for (Error error : result) {
                    Map<Integer, Error> node = errors.computeIfAbsent(error.getType(), k -> new HashMap<>());
                    node.put(error.getErrorCode(), error);
                }
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    public Error get(@NonNull String type, @NonNull Integer code) {
        if (errors.containsKey(type)) {
            return errors.get(type)
                    .get(code);
        }
        return null;
    }

    private static Errors __instance;

    public static Errors getDefault() {
        Preconditions.checkNotNull(__instance);
        return __instance;
    }

    public static Errors create(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                @NonNull BaseEnv<?> env) throws ConfigurationException {
        if (__instance == null) {
            try {
                __instance = new Errors();
                __instance.configure(xmlConfig, env);
            } catch (ConfigurationException ex) {
                __instance = null;
                throw ex;
            }
        }
        return __instance;
    }
}
