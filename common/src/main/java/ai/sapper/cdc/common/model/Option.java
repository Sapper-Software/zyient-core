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

package ai.sapper.cdc.common.model;

import ai.sapper.cdc.common.config.ConfigReader;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public class Option {
    public static class Constants {
        public static final String __CONFIG_PATH = "option";
        public static final String CONFIG_NAME = "name";
        public static final String CONFIG_TYPE = "type";
        public static final String CONFIG_VALUE = "value";
    }

    private String name;
    private String dataType;
    private String value;

    public Option read(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        name = config.getString(Constants.CONFIG_NAME);
        ConfigReader.checkStringValue(name, getClass(), Constants.CONFIG_NAME);
        dataType = config.getString(Constants.CONFIG_TYPE);
        ConfigReader.checkStringValue(dataType, getClass(), Constants.CONFIG_TYPE);
        value = config.getString(Constants.CONFIG_VALUE);
        ConfigReader.checkStringValue(value, getClass(), Constants.CONFIG_VALUE);
        return this;
    }

    public Object parseValue() {
        Object v = value;
        if (dataType.compareToIgnoreCase("boolean") == 0) {
            v = Boolean.parseBoolean(value);
        } else if (dataType.compareToIgnoreCase("long") == 0) {
            v = Long.parseLong(value);
        } else if (dataType.compareToIgnoreCase("int") == 0) {
            v = Integer.parseInt(value);
        } else if (dataType.compareToIgnoreCase("double") == 0) {
            v = Double.parseDouble(value);
        } else if (dataType.compareToIgnoreCase("short") == 0) {
            v = Short.parseShort(value);
        }
        return v;
    }
}
