package io.zyient.core.mapping.model;

import io.zyient.base.common.model.entity.IKey;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class LongIDKey implements IKey {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Override
    public String stringKey() {
        return String.valueOf(id);
    }

    @Override
    public int compareTo(IKey key) {
        if (key instanceof LongIDKey) {
            return (int) (getId() - ((LongIDKey) key).getId());
        }
        return -1;
    }

    @Override
    public IKey fromString(@NonNull String value) throws Exception {
        setId(Long.parseLong(value));
        return this;

    }
}
