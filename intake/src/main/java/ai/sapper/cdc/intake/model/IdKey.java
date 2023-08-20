package ai.sapper.cdc.intake.model;

import ai.sapper.cdc.common.model.entity.IKey;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.Column;
import javax.persistence.Embeddable;

@Getter
@Setter
@Embeddable
public class IdKey implements IKey {
    @Column(name = "id")
    private String id;

    public IdKey() {}

    public IdKey(@Nonnull String id) {
        this.id = id;
    }

    @Override
    public String stringKey() {
        return id;
    }

    @Override
    public int compareTo(IKey key) {
        if (key instanceof IdKey) {
            return id.compareTo(((IdKey) key).id);
        }
        return -1;
    }
}
