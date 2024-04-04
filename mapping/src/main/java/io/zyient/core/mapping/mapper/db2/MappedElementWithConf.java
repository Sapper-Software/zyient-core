package io.zyient.core.mapping.mapper.db2;

import io.zyient.core.mapping.model.mapping.MappedElement;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MappedElementWithConf extends MappedElement {
    private DBMappingConf conf;
}
