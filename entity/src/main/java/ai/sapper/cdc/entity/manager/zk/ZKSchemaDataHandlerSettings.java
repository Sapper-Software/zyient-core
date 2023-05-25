package ai.sapper.cdc.entity.manager.zk;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.entity.manager.SchemaDataHandlerSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ZKSchemaDataHandlerSettings extends SchemaDataHandlerSettings {
    @Config(name = "basePath")
    private String basePath;
}
