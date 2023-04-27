package ai.sapper.cdc.entity;

import ai.sapper.cdc.common.model.services.ConfigSource;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DomainConfigSource extends ConfigSource {
    private String setupClass;
    private String storeKey;
    private String envTypeClass;
}
