package ai.sapper.cdc.entity.manager.zk.model;

import ai.sapper.cdc.entity.schema.EntitySchema;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class ZkEntitySchema {
    private EntitySchema schema;
    private String zkEntityPath;
    private String zkSchemaPath;
}
