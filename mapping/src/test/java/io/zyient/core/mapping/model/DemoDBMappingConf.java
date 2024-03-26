package io.zyient.core.mapping.model;

import io.zyient.core.mapping.mapper.db2.DBMappingConf;
import io.zyient.core.mapping.mapper.db2.MappedElementWithConf;
import io.zyient.core.mapping.model.mapping.MappedElement;
import io.zyient.core.mapping.model.mapping.MappingType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "demo_mapping_conf")
public class DemoDBMappingConf extends DBMappingConf {
    @Column(name = "name")
    private String name;

    @Override
    public MappedElementWithConf as() throws Exception {
        MappedElementWithConf conf = new MappedElementWithConf();
        conf.setConf(this);
        conf.setMappingType(MappingType.Field);
        conf.setSourcePath(this.getSourcePath());
        conf.setTargetPath(this.getTargetPath());
        return conf;
    }
}
