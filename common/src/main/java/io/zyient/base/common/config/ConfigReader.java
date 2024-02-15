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

package io.zyient.base.common.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.Options;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.common.utils.beans.BeanUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.commons.configuration2.io.ClasspathLocationStrategy;
import org.apache.commons.configuration2.io.CombinedLocationStrategy;
import org.apache.commons.configuration2.io.FileLocationStrategy;
import org.apache.commons.configuration2.io.ProvidedURLLocationStrategy;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.*;

@Getter
@Accessors(fluent = true)
public class ConfigReader {
    public static final String __NODE_PARAMETERS = "parameters";
    public static final String __NODE_PARAMETER = "parameter";
    public static final String __PARAM_NAME = "name";
    public static final String __PARAM_VALUE = "value";
    public static final String __PARAM_VALUES = "values";

    private final HierarchicalConfiguration<ImmutableNode> config;
    private final Class<? extends Settings> type;
    private Settings settings;

    public ConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
        this.config = config;
        this.type = null;
    }

    public ConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                        @NonNull String path) {
        this.config = config.configurationAt(path);
        this.type = null;
    }

    public ConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                        @NonNull Class<? extends Settings> type) {
        String path = getPathAnnotation(type);
        if (!Strings.isNullOrEmpty(path)) {
            this.config = config.configurationAt(path);
        } else {
            this.config = config;
        }
        this.type = type;
    }

    public ConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                        String path,
                        @NonNull Class<? extends Settings> type) {
        if (!Strings.isNullOrEmpty(path))
            this.config = config.configurationAt(path);
        else {
            if (type.isAnnotationPresent(ConfigPath.class)) {
                ConfigPath cp = type.getAnnotation(ConfigPath.class);
                if (!Strings.isNullOrEmpty(cp.path())) {
                    this.config = config.configurationAt(cp.path());
                } else
                    this.config = config;
            } else
                this.config = config;
        }
        this.type = type;
    }

    public ConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                        @NonNull String path,
                        @NonNull Settings settings) {
        this.config = config.configurationAt(path);
        this.type = settings.getClass();
        this.settings = settings;
    }

    public String read(@NonNull String name) throws ConfigurationException {
        return read(name, true);
    }

    public String read(@NonNull String name, boolean required) throws ConfigurationException {
        String value = config.getString(name);
        if (required) {
            checkStringValue(value, getClass(), name);
        }
        return value;
    }

    public void read() throws ConfigurationException {
        Preconditions.checkNotNull(type);
        try {
            settings = type.getDeclaredConstructor().newInstance();
            settings = read(settings, config);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public Settings read(@NonNull Settings settings,
                         @NonNull HierarchicalConfiguration<ImmutableNode> config)
            throws ConfigurationException {
        Preconditions.checkNotNull(type);
        try {
            Field[] fields = ReflectionHelper.getAllFields(settings.getClass());
            if (fields != null) {
                settings = type.getDeclaredConstructor().newInstance();
                for (Field field : fields) {
                    if (field.isAnnotationPresent(Config.class)) {
                        Config c = field.getAnnotation(Config.class);
                        if (c.name().compareTo(Settings.CONFIG_PARAMS) == 0) {
                            Map<String, String> params = readParameters();
                            if (params != null)
                                ReflectionHelper.setValue(params, settings, field);
                            continue;
                        }
                        if (c.type().equals(Exists.class)) {
                            if (checkIfNodeExists(config, c.name())) {
                                ReflectionHelper.setBooleanValue(settings, field, true);
                            } else {
                                ReflectionHelper.setBooleanValue(settings, field, false);
                            }
                            continue;
                        }
                        if (checkIfNodeExists(config, c.name())) {
                            if (!c.parser().equals(ConfigValueParser.DummyValueParser.class)) {
                                String value = config.getString(c.name());
                                ConfigValueParser<?> parser = c.parser().getDeclaredConstructor().newInstance();
                                Object o = parser.parse(value);
                                if (o != null) {
                                    ReflectionHelper.setValue(o, settings, field);
                                } else if (c.required()) {
                                    throw new ConfigurationException(String.format("Required configuration not found. [name=%s]", c.name()));
                                }
                                continue;
                            }
                            if (!c.custom().equals(FieldValueParser.DummyParser.class)) {
                                FieldValueParser<?> parser = c.custom().getDeclaredConstructor()
                                        .newInstance();
                                Object value = parser.parse(config);
                                if (value == null) {
                                    if (c.required()) {
                                        throw new Exception(String.format("Required value not found. [name=%s]", c.name()));
                                    } else {
                                        continue;
                                    }
                                }
                                ReflectionHelper.setValue(value, settings, field);
                                continue;
                            }
                            if (c.type().equals(String.class)) {
                                ReflectionHelper.setStringValue(settings, field, config.getString(c.name()));
                            } else if (ReflectionHelper.isBoolean(c.type())) {
                                ReflectionHelper.setBooleanValue(settings, field, config.getBoolean(c.name()));
                            } else if (ReflectionHelper.isShort(c.type())) {
                                ReflectionHelper.setShortValue(settings, field, config.getShort(c.name()));
                            } else if (ReflectionHelper.isInt(c.type())) {
                                ReflectionHelper.setIntValue(settings, field, config.getInt(c.name()));
                            } else if (ReflectionHelper.isLong(c.type())) {
                                ReflectionHelper.setLongValue(settings, field, config.getLong(c.name()));
                            } else if (ReflectionHelper.isFloat(c.type())) {
                                ReflectionHelper.setFloatValue(settings, field, config.getFloat(c.name()));
                            } else if (ReflectionHelper.isDouble(c.type())) {
                                ReflectionHelper.setDoubleValue(settings, field, config.getDouble(c.name()));
                            } else if (c.type().equals(Options.class)) {
                                Options options = new Options(c.name());
                                options.read(config);
                                ReflectionHelper.setValue(options, settings, field);
                            } else if (c.type().equals(List.class)) {
                                List<String> values = readAsList(c.name(), String.class);
                                if (values != null) {
                                    ReflectionHelper.setValue(values, settings, field);
                                } else if (c.required()) {
                                    throw new ConfigurationException(String.format("Required configuration not found. [name=%s]", c.name()));
                                }
                            } else if (c.type().equals(Class.class)) {
                                String cname = config.getString(c.name());
                                Class<?> cls = Class.forName(cname);
                                ReflectionHelper.setValue(cls, settings, field);
                            } else if (c.type().isEnum()) {
                                String value = config.getString(c.name());
                                Enum<?> e = (Enum<?>) ReflectionHelper.getValueFromString(c.type(), value);
                                ReflectionHelper.setValue(e, settings, field);
                            } else if (c.type().equals(Map.class)) {
                                Map<String, String> map = readAsMap(config, c.name());
                                if (map != null) {
                                    ReflectionHelper.setValue(map, settings, field);
                                } else if (c.required()) {
                                    throw new ConfigurationException(String.format("Required configuration not found. [name=%s]", c.name()));
                                }
                            }
                            if (c.required()) {
                                Object v = ReflectionHelper.reflectionUtils().getFieldValue(settings, field);
                                if (v == null) {
                                    throw new ConfigurationException(String.format("Required configuration not found. [name=%s]", c.name()));
                                }
                            }
                        } else if (c.required()) {
                            throw new ConfigurationException(String.format("Required configuration not found. [name=%s]", c.name()));
                        }
                    }
                }
            }
            return settings;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public static <T> T read(@NonNull HierarchicalConfiguration<ImmutableNode> node,
                             Class<? extends T> type) throws Exception {
        T pojo = type.getDeclaredConstructor().newInstance();
        return read(node, pojo);
    }

    public static <T> T read(@NonNull HierarchicalConfiguration<ImmutableNode> node,
                             @NonNull T pojo) throws Exception {
        Field[] fields = ReflectionHelper.getAllFields(pojo.getClass());
        Preconditions.checkNotNull(fields);
        for (Field field : fields) {
            if (field.isAnnotationPresent(Config.class)) {
                Config cfg = field.getAnnotation(Config.class);
                if (!cfg.custom().equals(FieldValueParser.DummyParser.class)) {
                    FieldValueParser<?> parser = cfg.custom().getDeclaredConstructor()
                            .newInstance();
                    Object value = parser.parse(node);
                    if (value == null) {
                        if (cfg.required()) {
                            throw new Exception(String.format("Required value not found. [name=%s]", cfg.name()));
                        } else {
                            continue;
                        }
                    }
                    ReflectionHelper.setValue(value, pojo, field);
                    continue;
                }
                String value = node.getString(cfg.name());
                if (Strings.isNullOrEmpty(value)) {
                    if (cfg.required()) {
                        throw new Exception(String.format("Required value not found. [name=%s]", cfg.name()));
                    } else {
                        continue;
                    }
                }
                if (!cfg.parser().equals(ConfigValueParser.DummyValueParser.class)) {
                    ConfigValueParser<?> parser = cfg.parser()
                            .getDeclaredConstructor()
                            .newInstance();
                    Object v = parser.parse(value);
                    if (v == null) {
                        if (cfg.required()) {
                            throw new Exception(String.format("Required value not found. [name=%s][parser=%s]",
                                    cfg.name(), cfg.parser().getCanonicalName()));
                        } else {
                            continue;
                        }
                    }
                    ReflectionHelper.setValue(v, pojo, field);
                } else {
                    ReflectionHelper.setValueFromString(value, pojo, field);
                }
            }
        }
        return pojo;
    }

    public boolean checkIfNodeExists(String path, @NonNull String name) {
        String key = name;
        if (!Strings.isNullOrEmpty(path)) {
            key = String.format("%s.%s", path, key);
        }
        if (!Strings.isNullOrEmpty(key)) {
            try {
                List<HierarchicalConfiguration<ImmutableNode>> nodes = get().configurationsAt(key);
                if (nodes != null) return !nodes.isEmpty();
            } catch (ConfigurationRuntimeException e) {
                // Ignore Exception
            }
        }
        return false;
    }

    public HierarchicalConfiguration<ImmutableNode> get() {
        return config;
    }

    public HierarchicalConfiguration<ImmutableNode> get(@NonNull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        if (checkIfNodeExists((String) null, name))
            return config.configurationAt(name);
        return null;
    }

    public List<HierarchicalConfiguration<ImmutableNode>> getCollection(@NonNull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        if (checkIfNodeExists((String) null, name))
            return config.configurationsAt(name);
        return null;
    }

    public <T> List<T> readAsList(@NonNull String path,
                                  @NonNull Class<? extends T> type) throws Exception {
        Preconditions.checkArgument(ReflectionHelper.isPrimitiveTypeOrString(type));
        if (checkIfNodeExists(path, __PARAM_VALUES)) {
            String key = String.format("%s.%s", path, __PARAM_VALUES);
            HierarchicalConfiguration<ImmutableNode> pc = config.configurationAt(key);
            if (pc != null) {
                List<Object> values = pc.getList(__PARAM_VALUE);
                if (values != null) {
                    List<T> result = createListType(type);
                    for (Object value : values) {
                        addToList(value, result, type);
                    }
                    return result;
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private <T> void addToList(Object value, List<T> list, Class<? extends T> type) throws Exception {
        if (type.equals(String.class)) {
            list.add((T) value);
        } else if (ReflectionHelper.isShort(type)) {
            if (value instanceof String) {
                Short s = Short.parseShort((String) value);
                list.add((T) s);
            } else if (ReflectionHelper.isShort(value.getClass())) {
                list.add((T) value);
            } else {
                throw new Exception(
                        String.format("Cannot convert to Short. [type=%s]", value.getClass().getCanonicalName()));
            }
        } else if (ReflectionHelper.isInt(type)) {
            if (value instanceof String) {
                Integer s = Integer.parseInt((String) value);
                list.add((T) s);
            } else if (ReflectionHelper.isInt(value.getClass())) {
                list.add((T) value);
            } else {
                throw new Exception(
                        String.format("Cannot convert to Integer. [type=%s]", value.getClass().getCanonicalName()));
            }
        } else if (ReflectionHelper.isLong(type)) {
            if (value instanceof String) {
                Long s = Long.parseLong((String) value);
                list.add((T) s);
            } else if (ReflectionHelper.isLong(value.getClass())) {
                list.add((T) value);
            } else {
                throw new Exception(
                        String.format("Cannot convert to Long. [type=%s]", value.getClass().getCanonicalName()));
            }
        } else if (ReflectionHelper.isFloat(type)) {
            if (value instanceof String) {
                Float s = Float.parseFloat((String) value);
                list.add((T) s);
            } else if (ReflectionHelper.isFloat(value.getClass())) {
                list.add((T) value);
            } else {
                throw new Exception(
                        String.format("Cannot convert to Float. [type=%s]", value.getClass().getCanonicalName()));
            }
        } else if (ReflectionHelper.isDouble(type)) {
            if (value instanceof String) {
                Double s = Double.parseDouble((String) value);
                list.add((T) s);
            } else if (ReflectionHelper.isDouble(value.getClass())) {
                list.add((T) value);
            } else {
                throw new Exception(
                        String.format("Cannot convert to Double. [type=%s]", value.getClass().getCanonicalName()));
            }
        } else if (type.equals(Class.class)) {
            if (value instanceof String) {
                Class<?> cls = Class.forName((String) value);
                list.add((T) cls);
            } else {
                throw new Exception(
                        String.format("Cannot convert to Class. [type=%s]", value.getClass().getCanonicalName()));
            }
        } else
            throw new Exception(String.format("List type not supported. [type=%s]", type.getCanonicalName()));
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> createListType(Class<? extends T> type) throws Exception {
        if (ReflectionHelper.isShort(type)) {
            return (List<T>) new ArrayList<Short>();
        } else if (ReflectionHelper.isInt(type)) {
            return (List<T>) new ArrayList<Integer>();
        } else if (ReflectionHelper.isLong(type)) {
            return (List<T>) new ArrayList<Long>();
        } else if (ReflectionHelper.isFloat(type)) {
            return (List<T>) new ArrayList<Float>();
        } else if (ReflectionHelper.isDouble(type)) {
            return (List<T>) new ArrayList<Double>();
        } else if (type.equals(String.class)) {
            return (List<T>) new ArrayList<String>();
        } else if (type.equals(Class.class)) {
            return (List<T>) new ArrayList<Class>();
        }
        throw new Exception(String.format("List type not supported. [type=%s]", type.getCanonicalName()));
    }

    public Map<String, String> readParameters(@NonNull String node) {
        if (checkIfNodeExists((String) null, node)) {
            HierarchicalConfiguration<ImmutableNode> pc = config.configurationAt(__NODE_PARAMETERS);
            if (pc != null) {
                List<HierarchicalConfiguration<ImmutableNode>> pl = pc.configurationsAt(__NODE_PARAMETER);
                if (pl != null && !pl.isEmpty()) {
                    Map<String, String> params = new HashMap<>(pl.size());
                    for (HierarchicalConfiguration<ImmutableNode> p : pl) {
                        String name = p.getString(__PARAM_NAME);
                        if (!Strings.isNullOrEmpty(name)) {
                            String value = p.getString(__PARAM_VALUE);
                            params.put(name, value);
                        }
                    }
                    return params;
                }
            }
        }
        return null;
    }

    public Map<String, String> readParameters() {
        return readParameters(__NODE_PARAMETERS);
    }

    public static XMLConfiguration readFromFile(@NonNull String filename) throws ConfigurationException {
        File cf = new File(filename);
        if (!cf.exists()) {
            throw new ConfigurationException(String.format("Specified configuration file not found. [file=%s]", cf.getAbsolutePath()));
        }
        if (!cf.canRead()) {
            throw new ConfigurationException(String.format("Cannot read configuration file. [file=%s]", cf.getAbsolutePath()));
        }
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder =
                new FileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
                        .configure(params.xml()
                                .setFileName(cf.getAbsolutePath()));
        return builder.getConfiguration();
    }

    public static XMLConfiguration readFromClasspath(@NonNull String path) throws ConfigurationException {
        List<FileLocationStrategy> subs = Collections.singletonList(
                new ClasspathLocationStrategy());
        FileLocationStrategy strategy = new CombinedLocationStrategy(subs);
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder
                = new FileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
                .configure(params
                        .xml()
                        .setLocationStrategy(strategy)
                        .setURL(
                                ConfigReader.class.getClassLoader().getResource(path)
                        ));

        return builder.getConfiguration();
    }

    public static XMLConfiguration readFromURI(@NonNull String path) throws ConfigurationException {
        try {
            List<FileLocationStrategy> subs = Collections.singletonList(
                    new ProvidedURLLocationStrategy());
            FileLocationStrategy strategy = new CombinedLocationStrategy(subs);
            Parameters params = new Parameters();
            FileBasedConfigurationBuilder<XMLConfiguration> builder
                    = new FileBasedConfigurationBuilder<>(XMLConfiguration.class)
                    .configure(params
                            .xml()
                            .setLocationStrategy(strategy)
                            .setURL(
                                    new URI(path).toURL()
                            ));
            return builder.getConfiguration();
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public static XMLConfiguration read(@NonNull String path, @NonNull EConfigFileType type) throws ConfigurationException {
        switch (type) {
            case File:
                return readFromFile(path);
            case Remote:
                return readFromURI(path);
            case Resource:
                return readFromClasspath(path);
        }
        throw new ConfigurationException(String.format("Invalid Config File type. [type=%s]", type.name()));
    }

    public static boolean checkIfNodeExists(@NonNull HierarchicalConfiguration<ImmutableNode> node, @NonNull String name) {
        if (!Strings.isNullOrEmpty(name)) {
            try {
                List<HierarchicalConfiguration<ImmutableNode>> nodes = node.configurationsAt(name);
                if (nodes != null) return !nodes.isEmpty();
            } catch (ConfigurationRuntimeException e) {
                // Ignore Exception
            }
        }
        return false;
    }

    public static String getPathAnnotation(@NonNull Class<?> type) {
        if (type.isAnnotationPresent(ConfigPath.class)) {
            ConfigPath cp = type.getAnnotation(ConfigPath.class);
            if (!Strings.isNullOrEmpty(cp.path())) {
                return cp.path();
            } else {
                return type.getSimpleName();
            }
        }
        return null;
    }

    public static <T> void from(@NonNull T obj,
                         @NonNull Map<String, Object> properties,
                         boolean ignore) throws Exception {
        Field[] fields = ReflectionHelper.getAllFields(obj.getClass());
        if (fields != null) {
            for (Field field : fields) {
                if (!field.isAnnotationPresent(Config.class)) continue;
                Config cfg = field.getAnnotation(Config.class);
                Object value = properties.get(cfg.name());
                if (value != null) {
                    BeanUtils.setValue(obj, field.getName(), value);
                }
                if (!ignore) {
                    if (cfg.required()) {
                        value = BeanUtils.getValue(obj, field.getName());
                        if (value == null) {
                            throw new ConfigurationException(String.format("[type=%s] Missing field value: field=%s]",
                                    obj.getClass().getCanonicalName(), field.getName()));
                        }
                    }
                }
            }
        }
    }

    public static final String CONFIG_KEY_CLASS = "class";
    public static final String CONFIG_ATTR_TYPE = "type";

    public static Class<?> readType(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        return readType(config, CONFIG_ATTR_TYPE);
    }

    public static Class<?> readType(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                    @NonNull String key) throws Exception {
        key = String.format("[@%s]", key);
        String cls = config.getString(key);
        if (!Strings.isNullOrEmpty(cls))
            return Class.forName(cls);
        return null;
    }

    public static Class<?> readAsClass(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                       @NonNull String key) throws Exception {
        String cls = config.getString(key);
        return Class.forName(cls);
    }

    public static Class<?> readAsClass(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        return readAsClass(config, CONFIG_KEY_CLASS);
    }

    public static <T> Map<String, T> readAsMap(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                               @NonNull String key) {
        Map<String, T> map = new LinkedHashMap<>();
        Configuration subset = config.subset(key);
        if (!subset.isEmpty()) {
            Iterator<String> it = subset.getKeys();
            while (it.hasNext()) {
                String k = (String) it.next();
                //noinspection unchecked
                T v = (T) subset.getProperty(k);
                map.put(k, v);
            }
        }
        if (map.isEmpty()) return null;
        return map;
    }

    public static File readFileNode(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                    @NonNull String name) throws Exception {
        String value = config.getString(name);
        return readFileNode(value);
    }

    public static File readFileNode(@NonNull String path) throws Exception {
        if (!Strings.isNullOrEmpty(path)) {
            return PathUtils.readFile(path);
        }
        return null;
    }

    public static void checkStringValue(String value,
                                        @NonNull Class<?> caller,
                                        @NonNull String name) throws ConfigurationException {
        if (Strings.isNullOrEmpty(value)) {
            throw new ConfigurationException(
                    String.format("[%s] Missing configuration [name=%s]",
                            caller.getSimpleName(), name));
        }
    }

    public static void checkNotNull(Object value,
                                    @NonNull Class<?> caller,
                                    @NonNull String name) throws ConfigurationException {
        if (value == null) {
            throw new ConfigurationException(
                    String.format("[%s] Missing configuration [name=%s]",
                            caller.getSimpleName(), name));
        }
    }
}
