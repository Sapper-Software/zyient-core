package io.zyient.core.mapping.mapper.db2;


import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.EDataTypes;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.StringKey;
import io.zyient.core.mapping.model.mapping.MappedElement;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@MappedSuperclass
public abstract class DBMappingConf implements IEntity<StringKey> {
    @EmbeddedId
    private StringKey key;
    @Column(name = "source_path")
    private String sourcePath;
    @Column(name = "target_path")
    private String targetPath;
    @Enumerated(EnumType.STRING)
    @Column(name = "conf_type")
    private DbMappingConfType confType;
    @Column(name = "filter_id")
    private String filterId;
    @Column(name = "parent_id")
    private String parentId;

    @Override
    public int compare(StringKey key) {
        return this.key.compareTo(key);
    }

    @Override
    public IEntity<StringKey> copyChanges(IEntity<StringKey> source, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<StringKey> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    public StringKey entityKey() {
        return this.key;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }

    public abstract MappedElementWithConf as() throws Exception;
}
