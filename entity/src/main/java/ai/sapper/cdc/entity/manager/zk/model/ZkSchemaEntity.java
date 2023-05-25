package ai.sapper.cdc.entity.manager.zk.model;

import ai.sapper.cdc.entity.schema.SchemaEntity;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class ZkSchemaEntity {
    private SchemaEntity entity;
    private String zkPath;
}
