package io.zyient.core.mapping.model;

import io.zyient.base.common.model.entity.IKey;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@Embeddable
public class CustodianAccountKey implements IKey {
    @Column(name = "id")
    private Long id;
    @Override
    public String stringKey() {
        return String.valueOf(id);
    }

    @Override
    public int compareTo(IKey key) {
        if (key instanceof CustodianAccountKey) {
            return id.compareTo(((CustodianAccountKey) key).id);
        }
        return Short.MIN_VALUE;
    }

    @Override
    public IKey fromString(@NonNull String value) throws Exception {
        id = Long.parseLong(value);
        return this;
    }
}
