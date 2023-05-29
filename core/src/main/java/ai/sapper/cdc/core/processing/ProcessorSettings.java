package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ProcessorSettings extends Settings {
    public static final String __CONFIG_PATH = "processor";

    @Config(name = "name")
    private String name;
    @Config(name = "type", type = Class.class)
    private Class<? extends Processor<?, ?>> processorType;
}
